// Create the tiller_tools namespace if it doesn't exist
var tiller_tools = tiller_tools || {};

tiller_tools.stripe = tiller_tools.stripe || (function () {
  // Constants
  const STRIPE_PAYOUT_DESCRIPTION_PREFIX  = sb_getProperty('STRIPE_PAYOUT_DESCRIPTION_PREFIX')  || "Orig Co Name:stripe Orig ID:x8598";
  const STRIPE_INSTITUTION_NAME           = sb_getProperty('STRIPE_INSTITUTION_NAME')           || "Stripe";
  const STRIPE_PAYOUT_CATEGORY_LABEL      = sb_getProperty('STRIPE_PAYOUT_CATEGORY_LABEL')      || "Bank Account transfer";
  const STRIPE_FEE_CATEGORY_LABEL         = sb_getProperty('STRIPE_FEE_CATEGORY_LABEL')         || "Finance Fee";


  // Initialize tracking variables
  let rowsModified = [];    // Array to track modified or added rows
  let filesAdded = [];      // Array to track files added to Google Drive
  let urlsAccessed = [];    // Array to track URLs accessed during the script execution

  /**
   * Helper function to find the column index by its name.
   * @param {Array} headers - Array of header names from the sheet.
   * @param {string} columnName - The name of the column to find.
   * @returns {number} - The 1-based index of the column.
   */
  const findColumnIndex = (headers, columnName) => {
    const index = headers.findIndex(header =>
      (typeof header === 'string' ? header : String(header)).toLowerCase() === columnName.toLowerCase()
    );
    if (index === -1) throw new Error(`Column '${columnName}' not found in the sheet headers.`);
    return index + 1; // 1-based index for Sheets API
  };

  /**
   * Main function to process Stripe payouts.
   * Fetches payouts from Stripe and processes each payout to decompose it into individual transactions.
   * @param {Date|string|null} startDateInput - Start date as Date object, string, or null for no limit.
   * @param {Date|string|null} endDateInput - End date as Date object, string, or null for no limit.
   */
  const processStripePayouts = (startDateInput = null, endDateInput = null) => {
    const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName("Transactions");
    if (!sheet) throw new Error("Sheet 'Transactions' not found. Please check the sheet name.");

    const headers = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];

    let payoutsWithTransactions = []; // Initialize collection

    try {
      sidebar.log("Starting to process Stripe payouts.");

      // Ensure all required columns exist
      const dateColumnIndex = findColumnIndex(headers, "Date");
      const descriptionColumnIndex = findColumnIndex(headers, "Description");
      const amountColumnIndex = findColumnIndex(headers, "Amount");
      const receiptUrlColumnIndex = findColumnIndex(headers, "ReceiptURL");

      const columnIndices = {
        date: dateColumnIndex,
        description: descriptionColumnIndex,
        amount: amountColumnIndex,
        receiptUrl: receiptUrlColumnIndex,
        institution: findColumnIndex(headers, "Institution"),
        accountNumber: findColumnIndex(headers, "Account #"),
        accountId: findColumnIndex(headers, "Account ID"),
        category: findColumnIndex(headers, "Category"),
        transactionId: findColumnIndex(headers, "Transaction ID") // Added Transaction ID column
      };

      sidebar.log(`Found required columns: Date (${dateColumnIndex}), Description (${descriptionColumnIndex}), Amount (${amountColumnIndex}), ReceiptURL (${receiptUrlColumnIndex}).`);

      // Parse start and end dates
      sidebar.log(`Parsing date range with startDateInput: ${startDateInput}, endDateInput: ${endDateInput}`);
      const { startDate, endDate } = parseDateRange(startDateInput, endDateInput);

      sidebar.log(`Processing payouts from ${startDate ? startDate.toISOString().split('T')[0] : 'beginning'} to ${endDate ? endDate.toISOString().split('T')[0] : 'present'}.`);

      // Filter payout rows from the sheet within the date range
      const payoutRows = filterStripePayoutRows(sheet, dateColumnIndex, descriptionColumnIndex, amountColumnIndex, startDate, endDate);
      sidebar.log(`Found ${payoutRows.length} payout rows in the sheet within the date range.`);

      // Fetch payouts from Stripe within the date range
      const stripePayouts = fetchStripePayouts(startDate, endDate);
      sidebar.log(`Fetched ${stripePayouts.length} payouts from Stripe within the date range.`);

      // Map to store payoutId to payoutRow for easy access
      const payoutRowMap = new Map();


      // Prepare for batch fetching transactions
      let payoutIdsToProcess = [];

      // Find matching payouts and collect payout IDs
      payoutRows.forEach(payoutRow => {
        const matchingPayout = findMatchingPayout(payoutRow, stripePayouts);
        if (matchingPayout) {
          sidebar.log(`Found matching payout ID: ${matchingPayout.id} for row ${payoutRow.rowIndex}`);
          payoutRowMap.set(matchingPayout.id, {
            payoutRow: payoutRow,
            matchingPayout: matchingPayout
          });
          payoutIdsToProcess.push(matchingPayout.id);
        } else {
          sidebar.log(`No matching payout found for row ${payoutRow.rowIndex}, Date: ${payoutRow.date}, Description: ${payoutRow.description}`);
        }
      });

      // Fetch transactions for all payouts in parallel
      const payoutsTransactionsMap = fetchTransactionsForPayouts(payoutIdsToProcess);

      // Process each payout's transactions
      payoutIdsToProcess.forEach(payoutId => {
        const { payoutRow, matchingPayout } = payoutRowMap.get(payoutId);
        const transactions = payoutsTransactionsMap[payoutId];
        if (transactions) {
          processPayoutTransactions(sheet, payoutRow, matchingPayout, transactions, headers, columnIndices);

          // Populate payoutsWithTransactions with the current payout and its transactions
          payoutsWithTransactions.push({
            payoutId: payoutId,
            payoutRow: payoutRow,
            matchingPayout: matchingPayout,
            transactions: transactions
          });
        } else {
          sidebar.log(`No transactions found for payout ID: ${payoutId}`);
        }
      });

    } catch (error) {
      // Enrich error messages with context
      sidebar.error(`Error in processStripePayouts: ${error.message}`);
      Logger.log(`Error in processStripePayouts: ${error.message}`);
      SpreadsheetApp.getUi().alert(`Error processing Stripe payouts: ${error.message}`);
    } finally {
      if (payoutsWithTransactions.length > 0) {
        compileAndSendSummaryEmail(payoutsWithTransactions);
      } else {
        sidebar.log("No payouts were processed. No summary email will be sent.");
      }
    }
  };

  /**
   * Parses the start and end date inputs into Date objects.
   * If null, there is no limit on the respective date.
   * @param {Date|string|null} startDateInput - Start date input, can be Date object, string, or null.
   * @param {Date|string|null} endDateInput - End date input, can be Date object, string, or null.
   * @returns {Object} - Object containing startDate and endDate as Date objects or null.
   */
  const parseDateRange = (startDateInput, endDateInput) => {
    sidebar.log("Starting parseDateRange function.");
    let startDate = null;
    let endDate = null;

    // Parse start date
    if (startDateInput) {
      if (startDateInput instanceof Date) {
        startDate = startDateInput;
      } else if (typeof startDateInput === 'string') {
        startDate = parseDateString(startDateInput);
      } else {
        throw new Error(`Invalid start date input: ${startDateInput}`);
      }
      if (isNaN(startDate.getTime())) {
        throw new Error(`Invalid start date format: ${startDateInput}`);
      }
      sidebar.log(`Parsed startDate: ${startDate.toISOString()}`);
    } else {
      sidebar.log("No startDateInput provided; startDate is null (no lower limit).");
    }

    // Parse end date
    if (endDateInput) {
      if (endDateInput instanceof Date) {
        endDate = endDateInput;
      } else if (typeof endDateInput === 'string') {
        endDate = parseDateString(endDateInput);
      } else {
        throw new Error(`Invalid end date input: ${endDateInput}`);
      }
      if (isNaN(endDate.getTime())) {
        throw new Error(`Invalid end date format: ${endDateInput}`);
      }
      sidebar.log(`Parsed endDate: ${endDate.toISOString()}`);
    } else {
      sidebar.log("No endDateInput provided; endDate is null (no upper limit).");
    }

    return { startDate, endDate };
  };

  /**
   * Parses a date string into a Date object, assuming local time unless 'T' or 'Z' is present.
   * @param {string} dateStr - The date string to parse.
   * @returns {Date} - The parsed Date object.
   */
  const parseDateString = (dateStr) => {
    if (dateStr.includes('T') || dateStr.includes('Z')) {
      return new Date(dateStr);
    } else {
      // Parse as local date
      const parts = dateStr.split('-');
      if (parts.length !== 3) {
        throw new Error(`Invalid date format: ${dateStr}`);
      }
      const [year, month, day] = parts.map(Number);
      if (!year || !month || !day) {
        throw new Error(`Invalid date components in date string: ${dateStr}`);
      }
      // In JavaScript, months are 0-based, so subtract 1 from month
      return new Date(year, month - 1, day);
    }
  };

  /**
   * Filters the payout rows from the sheet based on the description prefix and date range.
   * @param {GoogleAppsScript.Spreadsheet.Sheet} sheet - The sheet containing transactions.
   * @param {number} dateColumnIndex - The index of the Date column.
   * @param {number} descriptionColumnIndex - The index of the Description column.
   * @param {number} amountColumnIndex - The index of the Amount column.
   * @param {Date|null} startDate - The start date for filtering or null for no limit.
   * @param {Date|null} endDate - The end date for filtering or null for no limit.
   * @returns {Array} - An array of payout row objects.
   */
  const filterStripePayoutRows = (sheet, dateColumnIndex, descriptionColumnIndex, amountColumnIndex, startDate, endDate) => {
    const data = sheet.getDataRange().getValues();
    try {
      return data.slice(1).reduce((acc, row, index) => {
        const description = row[descriptionColumnIndex - 1];
        const dateValue = row[dateColumnIndex - 1];

        if (description && description.startsWith(STRIPE_PAYOUT_DESCRIPTION_PREFIX) && dateValue instanceof Date) {
          const rowDate = new Date(dateValue);
          rowDate.setHours(0, 0, 0, 0);

          // Apply date filters
          if ((!startDate || rowDate >= startDate) && (!endDate || rowDate <= endDate)) {
            acc.push({
              rowIndex: index + 2,
              date: dateValue,
              amount: row[amountColumnIndex - 1],
              description: description
            });
          }
        }
        return acc;
      }, []);
    } catch (error) {
      throw new Error(`Error filtering payout rows: ${error.message}`);
    }
  };

  /**
   * Fetches payouts from Stripe using the Stripe API, filtered by date range.
   * @param {Date|null} startDate - The start date for filtering or null for no limit.
   * @param {Date|null} endDate - The end date for filtering or null for no limit.
   * @returns {Array} - An array of payout objects from Stripe.
   */
  const fetchStripePayouts = (startDate, endDate) => {
    const apiKey = sb_getProperty('STRIPE_API_KEY');
    if (!apiKey) throw new Error("Stripe API key not found in script properties.");

    let url = 'https://api.stripe.com/v1/payouts?limit=100';

    // Build the date filter parameters
    let dateParams = '';
    if (startDate) {
      const startTimestamp = Math.floor(startDate.getTime() / 1000);
      dateParams += `&created[gte]=${startTimestamp}`;
    }
    if (endDate) {
      const endTimestamp = Math.floor(endDate.getTime() / 1000);
      dateParams += `&created[lte]=${endTimestamp}`;
    }

    url += dateParams;
    urlsAccessed.push(url);
    sidebar.log(`Accessed URL: ${url}`);

    const options = {
      method: 'GET',
      headers: {
        'Authorization': `Bearer ${apiKey}`,
      },
      muteHttpExceptions: true
    };

    try {
      let allPayouts = [];
      let hasMore = true;

      while (hasMore) {
        const response = UrlFetchApp.fetch(url, options);
        if (response.getResponseCode() !== 200) {
          throw new Error(`Failed to fetch payouts: ${response.getContentText()}`);
        }

        const result = JSON.parse(response.getContentText());
        allPayouts = allPayouts.concat(result.data);

        sidebar.log(`Fetched ${result.data.length} payouts from URL: ${url}`);

        if (result.has_more) {
          const lastPayoutId = result.data[result.data.length - 1].id;
          url = `https://api.stripe.com/v1/payouts?limit=100&starting_after=${lastPayoutId}${dateParams}`;
          urlsAccessed.push(url);
          sidebar.log(`Accessed URL: ${url}`);
        } else {
          hasMore = false;
        }
      }

      sidebar.log(`Total payouts fetched: ${allPayouts.length}`);
      return allPayouts;

    } catch (error) {
      throw new Error(`Error fetching Stripe payouts from URL: ${url}, with headers: ${JSON.stringify(options.headers)}. Error: ${error.message}`);
    }
  };

  /**
   * Finds a matching payout from Stripe payouts based on the payout row data.
   * @param {Object} payoutRow - The payout row object from the sheet.
   * @param {Array} stripePayouts - Array of payout objects fetched from Stripe.
   * @returns {Object|null} - The matching payout object or null if not found.
   */
  const findMatchingPayout = (payoutRow, stripePayouts) => {
    try {
      const rowDate = new Date(payoutRow.date);
      rowDate.setHours(0, 0, 0, 0);
      const rowAmount = payoutRow.amount;

      return stripePayouts.find(payout => {
        const payoutDate = new Date((payout.arrival_date + 8 * 3600) * 1000); // Convert to PDT
        payoutDate.setHours(0, 0, 0, 0);
        return payoutDate.getTime() === rowDate.getTime() &&
          Math.abs(payout.amount / 100 - rowAmount) < 0.01;
      }) || null;

    } catch (error) {
      throw new Error(`Error finding matching payout for row: ${JSON.stringify(payoutRow)}. Error: ${error.message}`);
    }
  };

  /**
   * Fetches transactions for multiple payouts in parallel using fetchAll.
   * @param {Array} payoutIds - Array of payout IDs to fetch transactions for.
   * @returns {Object} - Map of payoutId to transactions array.
   */
  const fetchTransactionsForPayouts = (payoutIds) => {
    const apiKey = sb_getProperty('STRIPE_API_KEY');
    if (!apiKey) throw new Error("Stripe API key not found in script properties.");

    const payoutsTransactionsMap = {};

    // Prepare initial requests for all payouts
    let requestInfos = payoutIds.map(payoutId => {
      const url = `https://api.stripe.com/v1/balance_transactions?limit=100&payout=${payoutId}`;
      urlsAccessed.push(url);
      sidebar.log(`Accessed URL: ${url}`);

      const requestOptions = {
        url: url,
        method: 'GET',
        headers: {
          'Authorization': `Bearer ${apiKey}`,
        },
        muteHttpExceptions: true
      };

      return {
        payoutId: payoutId,
        requestOptions: requestOptions
      };
    });

    try {
      // Fetch initial pages in parallel
      let responses = UrlFetchApp.fetchAll(requestInfos.map(info => info.requestOptions));

      // Process responses and handle pagination
      let paginationInfos = [];

      responses.forEach((response, index) => {
        const payoutId = requestInfos[index].payoutId;
        if (response.getResponseCode() === 200) {
          const result = JSON.parse(response.getContentText());
          payoutsTransactionsMap[payoutId] = result.data;

          // Check for pagination
          if (result.has_more) {
            const lastTransactionId = result.data[result.data.length - 1].id;
            paginationInfos.push({
              payoutId: payoutId,
              starting_after: lastTransactionId
            });
          }
        } else {
          throw new Error(`Failed to fetch transactions for payout ${payoutId}: ${response.getContentText()}`);
        }
      });

      // Handle pagination
      while (paginationInfos.length > 0) {
        let nextRequestInfos = paginationInfos.map(paginationInfo => {
          const { payoutId, starting_after } = paginationInfo;
          const url = `https://api.stripe.com/v1/balance_transactions?limit=100&payout=${payoutId}&starting_after=${starting_after}`;
          urlsAccessed.push(url);
          sidebar.log(`Accessed URL: ${url}`);

          const requestOptions = {
            url: url,
            method: 'GET',
            headers: {
              'Authorization': `Bearer ${apiKey}`,
            },
            muteHttpExceptions: true
          };

          return {
            payoutId: payoutId,
            requestOptions: requestOptions
          };
        });

        let fetchRequests = nextRequestInfos.map(info => info.requestOptions);
        let fetchResponses = UrlFetchApp.fetchAll(fetchRequests);

        paginationInfos = []; // Reset for the next loop

        fetchResponses.forEach((response, index) => {
          const payoutId = nextRequestInfos[index].payoutId;
          if (response.getResponseCode() === 200) {
            const result = JSON.parse(response.getContentText());
            payoutsTransactionsMap[payoutId] = payoutsTransactionsMap[payoutId].concat(result.data);

            // Check for more pages
            if (result.has_more) {
              const lastTransactionId = result.data[result.data.length - 1].id;
              paginationInfos.push({
                payoutId: payoutId,
                starting_after: lastTransactionId
              });
            }
          } else {
            throw new Error(`Failed to fetch transactions for payout ${payoutId}: ${response.getContentText()}`);
          }
        });
      }

      return payoutsTransactionsMap;

    } catch (error) {
      throw new Error(`Error fetching transactions for payouts: ${error.message}`);
    }
  };

  /**
   * Processes transactions associated with a payout, decomposing them into individual transactions and fees.
   * Updates the sheet and saves receipts to Google Drive if applicable.
   * @param {GoogleAppsScript.Spreadsheet.Sheet} sheet - The sheet containing transactions.
   * @param {Object} payoutRow - The payout row object from the sheet.
   * @param {Object} payout - The payout object from Stripe.
   * @param {Array} transactions - Array of transactions for the payout.
   * @param {Array} headers - Array of header names from the sheet.
   * @param {Object} columnIndices - Object containing indices of required columns.
   */
  const processPayoutTransactions = (sheet, payoutRow, payout, transactions, headers, columnIndices) => {

    sidebar.log("Starting processing transactions") ;

    if (!sheet || !payoutRow || !payout || !headers || !columnIndices || !transactions) {
      throw new Error("Invalid arguments provided to processPayoutTransactions.");
    }

    const descriptionColumnIndex = columnIndices.description;
    const payoutRowData = sheet.getRange(payoutRow.rowIndex, 1, 1, headers.length).getValues()[0];
    const originalPayoutDescription = payoutRowData[descriptionColumnIndex - 1];

    const currentColumnIndices = Object.fromEntries(
      Object.entries(columnIndices).map(([key, index]) => [key, index - 1])
    );


    let newRowsData = [];
    let filesCreated = []; // Keep track of files created
    const originalDataRange = sheet.getRange(payoutRow.rowIndex, 1, sheet.getLastRow() - payoutRow.rowIndex + 1, headers.length);
    const originalData = originalDataRange.getValues();

    try {
      // Collect all charge IDs to fetch charges in parallel
      let chargeIds = [];
      let transactionChargeMap = {}; // Map transaction ID to charge ID

      // Begin processing transactions
      for (let transaction of transactions) {
        if (transaction.status !== 'available') {
          continue; // Skip transactions that are not available
        }

        if (!transaction.source) {
          // If transaction.source is null, use the source of the payout
          transaction.source = payout.id;
          sidebar.log(`Transaction ${transaction.id} has null source. Using payout ID ${payout.id} as source.`);
        }

        if (transaction.source.startsWith('ch_')) {
          // Proceed to collect charge IDs
          chargeIds.push(transaction.source);
          transactionChargeMap[transaction.id] = transaction.source;
        } else {
          // For other types of sources, proceed accordingly
          sidebar.log(`Transaction ${transaction.id} has non-charge source ${transaction.source}. Proceeding without charge data.`);
          // We can still process the transaction without charge data
          transaction.customer_name = null;
          transaction.receipt_url = null;
        }
      }

      // Fetch charges in parallel
      const chargesMap = fetchChargesInParallel(chargeIds);

      // Collect all receipt URLs for batch fetching
      let receiptUrls = [];
      let transactionReceiptsMap = {}; // Map transaction ID to receipt URL

      // Now process each transaction with charge data
      for (let transaction of transactions) {
        if (transaction.status !== 'available') {
          continue; // Skip transactions that are not available
        }

        const transactionAmount = transaction.amount / 100 || 0; // Set amount to zero if undefined

        let receiptUrl = null;
        let customerName = null;
        let reportingCategory = transaction.reporting_category || '';
        let description = transaction.description || '';

        const chargeId = transactionChargeMap[transaction.id];
        if (chargeId && chargesMap[chargeId]) {
          const chargeData = chargesMap[chargeId];

          // Get receipt URL and customer name
          receiptUrl = chargeData.receipt_url || null;
          if (chargeData.metadata && chargeData.metadata.customer_name) {
            customerName = chargeData.metadata.customer_name;
          }

          if (receiptUrl) {
            receiptUrls.push(receiptUrl);
            transactionReceiptsMap[transaction.id] = receiptUrl;
          }

          // Update transaction details
          transaction.customer_name = customerName;
          transaction.reporting_category = reportingCategory;
          transaction.description = description;
          transaction.receipt_url = receiptUrl;
        } else {
          // If charge data is not available, proceed without it
          sidebar.log(`Charge data not available for transaction ${transaction.id}. Proceeding without charge data.`);
          transaction.customer_name = null;
          transaction.receipt_url = null;
        }

      }

      // Fetch all receipts in parallel
      const receiptFilesMap = fetchAndSaveReceipts(receiptUrls, transactions, filesCreated);

      // Now create rows for transactions
      for (let transaction of transactions) {
        if (transaction.status !== 'available') {
          continue; // Skip transactions that are not available
        }
        const transactionAmount = (transaction.amount / 100) || 0; // Set amount to zero if undefined

        // Get saved receipt file URL
        let receiptDriveUrl = null;
        if (transaction.receipt_url && receiptFilesMap[transaction.receipt_url]) {
          receiptDriveUrl = receiptFilesMap[transaction.receipt_url];
        }

        // Get the Stripe account number from the payout object
        const stripeAccountNumber = payout.destination || '';

        // Determine the category
        const category = transaction.reporting_category === 'payout' ? STRIPE_PAYOUT_CATEGORY_LABEL : (payoutRowData[currentColumnIndices.category] || '');

        // Create transaction row
        const transactionRow = createTransactionRow(  payoutRowData,
                                                      transaction,
                                                      currentColumnIndices,
                                                      originalPayoutDescription,
                                                      transactionAmount,
                                                      receiptDriveUrl,
                                                      category,
                                                      stripeAccountNumber
                                                    );
        newRowsData.push(transactionRow);

        sidebar.log(`Inserted transaction row for amount:${transactionRow[currentColumnIndices.amount]} transaction ID:${transaction.id}, Row Index:${payoutRow.rowIndex + newRowsData.length}`);

        if (transaction.fee_details && transaction.fee_details.length > 0) {
          for (let fee of transaction.fee_details) {
            const feeAmount = fee.amount / 100 || 0;
            const feeRow = createFeeRow(  payoutRowData,
                                          fee,
                                          transaction,
                                          currentColumnIndices,
                                          originalPayoutDescription,
                                          feeAmount,
                                          receiptDriveUrl,
                                          category,
                                          stripeAccountNumber
                                        );
            newRowsData.push(feeRow);

            sidebar.log(`Inserted fee row for fee type: ${fee.type}, Row Index: ${payoutRow.rowIndex + newRowsData.length}`);
          }
        }
      }

      // Calculate the total processed amount by summing amounts from newRowsData
      const totalProcessedAmount = newRowsData.reduce((sum, row) => {
          const amount = row[currentColumnIndices.amount];
          return sum + (typeof amount === 'number' ? amount : 0);
        }, 0);

      // Log for debugging
      sidebar.log(`Total processed amount from newRowsData: ${totalProcessedAmount.toFixed(2)}`);

      // Now the totalProcessedAmount should match payoutRow.amount
      if (Math.abs(totalProcessedAmount) > 0.01) {
        const errorMessage = `Total processed amount (${totalProcessedAmount.toFixed(2)}) does not match payout amount (${payoutRow.amount.toFixed(2)}). Transactions will not be inserted.`;
        sidebar.log(errorMessage);
        Logger.log(errorMessage);

        // Delete any files created during processing
        deleteFiles(filesCreated);

        throw new Error(`Transaction processing failed for payout on Date: ${payoutRow.date}, Description: ${payoutRow.description}. Total amount mismatch.`);
      } else {
        // Insert new rows and update data
        if (newRowsData.length > 0) {
          sheet.insertRowsAfter(payoutRow.rowIndex, newRowsData.length);
          sheet.getRange(payoutRow.rowIndex + 1, 1, newRowsData.length, headers.length).setValues(newRowsData);
          sidebar.log(`Inserted ${newRowsData.length} new rows after row ${payoutRow.rowIndex}`);
        }

        // // Update the payout row's Amount to zero
        // sheet.getRange(payoutRow.rowIndex, columnIndices.amount).setValue(0);
        // sidebar.log(`Set payout row amount to zero for row ${payoutRow.rowIndex}`);

        sidebar.log(`Total processed amount matches the payout amount.`);
      }

    } catch (error) {
      const errorMessage = `Error processing payout for row ${payoutRow.rowIndex}: ${error.message}`;
      sidebar.error(errorMessage);
      Logger.log(errorMessage);

      // Delete any files created during processing
      deleteFiles(filesCreated);

      // Rollback any changes
      rollbackChanges(sheet, payoutRow.rowIndex, newRowsData.length, originalData);

      throw new Error(`Transaction processing failed for Date: ${payoutRow.date}, Description: ${payoutRow.description}. Error: ${error.message}`);
    }


    sidebar.log("Finished processing transactions") ;
  };

  /**
   * Fetches charges in parallel using fetchAll.
   * @param {Array} chargeIds - Array of charge IDs to fetch.
   * @returns {Object} - Map of chargeId to charge data.
   */
  const fetchChargesInParallel = (chargeIds) => {
    const apiKey = PropertiesService.getScriptProperties().getProperty('STRIPE_API_KEY');
    if (!apiKey) throw new Error("Stripe API key not found in script properties.");

    const uniqueChargeIds = [...new Set(chargeIds)];
    const requests = uniqueChargeIds.map(chargeId => {
      const url = `https://api.stripe.com/v1/charges/${chargeId}`;
      urlsAccessed.push(url);
      sidebar.log(`Accessed URL: ${url}`);
      return {
        url: url,
        method: 'GET',
        headers: {
          'Authorization': `Bearer ${apiKey}`,
        },
        muteHttpExceptions: true
      };
    });

    const chargesMap = {};

    try {
      const responses = UrlFetchApp.fetchAll(requests);

      responses.forEach((response, index) => {
        const chargeId = uniqueChargeIds[index];
        if (response.getResponseCode() === 200) {
          const chargeData = JSON.parse(response.getContentText());
          chargesMap[chargeId] = chargeData;
        } else {
          sidebar.log(`Failed to fetch charge ${chargeId}: ${response.getContentText()}`);
        }
      });

      return chargesMap;

    } catch (error) {
      throw new Error(`Error fetching charges in parallel: ${error.message}`);
    }
  };

  /**
   * Fetches and saves multiple receipts in parallel using fetchAll.
   * @param {Array} receiptUrls - Array of receipt URLs to fetch.
   * @param {Array} transactions - Array of transactions (for metadata).
   * @param {Array} filesCreated - Array to keep track of created file IDs.
   * @returns {Object} - Map of receiptUrl to Drive file URL.
   */
  const fetchAndSaveReceipts = (receiptUrls, transactions, filesCreated) => {
    const uniqueReceiptUrls = [...new Set(receiptUrls)];

    const requests = uniqueReceiptUrls.map(receiptUrl => {
      urlsAccessed.push(receiptUrl);
      sidebar.log(`Accessed URL: ${receiptUrl}`);
      return {
        url: receiptUrl,
        method: 'GET',
        muteHttpExceptions: true
      };
    });

    const receiptFilesMap = {};

    try {
      const responses = UrlFetchApp.fetchAll(requests);

      responses.forEach((response, index) => {
        const receiptUrl = uniqueReceiptUrls[index];
        if (response.getResponseCode() === 200) {
          const contentTypeHeader = response.getHeaders()['Content-Type'];
          const mimeType = contentTypeHeader.split(';')[0].trim();
          const receiptContent = response.getBlob();

          // Find a transaction associated with this receiptUrl for metadata
          const transaction = transactions.find(tx => tx.receipt_url === receiptUrl);

          // Save the receipt to Google Drive
          const saveResult = saveReceiptToDrive(receiptContent, transaction, mimeType);
          const receiptDriveUrl = saveResult.fileUrl;
          const fileId = saveResult.fileId;

          // Add fileId to filesCreated
          if (fileId) {
            filesCreated.push(fileId);
          }

          // Map the receiptUrl to the saved file URL
          receiptFilesMap[receiptUrl] = receiptDriveUrl;

          // Log the file added
          filesAdded.push({
            transactionId: transaction.id,
            fileUrl: receiptDriveUrl,
          });
          sidebar.log(`Saved receipt for transaction ID: ${transaction.id}, File URL: ${receiptDriveUrl}`);

        } else {
          sidebar.log(`Failed to fetch receipt from URL: ${receiptUrl}, Response Code: ${response.getResponseCode()}`);
        }
      });

      return receiptFilesMap;

    } catch (error) {
      sidebar.error(`Error fetching and saving receipts in batch: ${error.message}`);
      throw new Error(`Error fetching and saving receipts in batch: ${error.message}`);
    }
  };

  /**
   * Deletes files from Google Drive.
   * @param {Array} filesCreated - Array of file IDs that were created during processing.
   */
  const deleteFiles = (filesCreated) => {
    filesCreated.forEach(fileId => {
      try {
        DriveApp.getFileById(fileId).setTrashed(true);
        sidebar.log(`Deleted file with ID: ${fileId}`);
      } catch (err) {
        sidebar.log(`Failed to delete file with ID: ${fileId}. Error: ${err.message}`);
      }
    });
  };

  /**
   * Rollback function to restore the original state of the sheet in case of errors.
   * @param {GoogleAppsScript.Spreadsheet.Sheet} sheet - The sheet containing transactions.
   * @param {number} payoutRowIndex - The index of the payout row.
   * @param {number} newRowsCount - The number of new rows inserted.
   * @param {Array} originalData - The original data to restore.
   */
  const rollbackChanges = (sheet, payoutRowIndex, newRowsCount, originalData) => {
    try {
      // Delete new rows if any were inserted
      if (newRowsCount > 0) {
        sheet.deleteRows(payoutRowIndex + 1, newRowsCount);
        sidebar.log(`Rolled back: Deleted ${newRowsCount} new rows starting from row ${payoutRowIndex + 1}.`);
      }

      // Restore original data starting from payoutRowIndex
      sheet.getRange(payoutRowIndex, 1, originalData.length, originalData[0].length).setValues(originalData);
      sidebar.log(`Rolled back: Restored original data starting from row ${payoutRowIndex}.`);

    } catch (error) {
      throw new Error(`Rollback failed for row ${payoutRowIndex}. Error: ${error.message}`);
    }
  };

  /**
   * Compiles and sends a summary email with the payout and transaction details.
   * @param {Object} columnIndices - Object containing indices of required columns.
   * @param {Array} payoutsWithTransactions - Array of payouts and their associated transactions.
   */
  const compileAndSendSummaryEmail = ( payoutsWithTransactions) => {
    const emailAddress = sb_getProperty('SUMMARY_EMAIL') || 'finance@fortifiedstrength.org';
    const subject = 'Stripe Updates to Transactions';

    let summary = 'Summary of Stripe Payouts and Transactions:\n\n';

    // Include overall statistics
    summary += `Total Payouts Processed: ${payoutsWithTransactions.length}\n`;
    summary += `Files Added: ${filesAdded.length}\n`;
    summary += `URLs Accessed: ${urlsAccessed.length}\n\n`;

    // Detailed information per payout
    payoutsWithTransactions.forEach((payoutEntry, payoutIndex) => {
      const { payoutId, payoutRow, matchingPayout, transactions } = payoutEntry;
      summary += `Payout ${payoutIndex + 1}:\n`;
      summary += `  Payout ID: ${payoutId}\n`;
      summary += `  Date: ${payoutRow.date instanceof Date ? payoutRow.date.toISOString().split('T')[0] : 'Invalid Date'}\n`;
      summary += `  Amount: $${Number(payoutRow.amount).toFixed(2)}\n`;
      summary += `  Description: ${payoutRow.description}\n`;
      summary += `  Institution: ${payoutRow.institution || 'N/A'}\n`;
      summary += `  Account Number: ${payoutRow.accountNumber || 'N/A'}\n`;
      summary += `  Category: ${payoutRow.category || 'N/A'}\n`;
      summary += `  Transactions (${transactions.length}):\n`;

      transactions.forEach((transaction, txnIndex) => {
        const transactionAmount = (transaction.amount / 100).toFixed(2);
        const transactionDate = new Date(transaction.created * 1000).toISOString().split('T')[0];
        const transactionDescription = transaction.description || 'No Description';
        const transactionType = transaction.type || 'N/A';
        const customerName = transaction.customer_name || 'N/A';
        const receiptUrl = transaction.receipt_url || 'N/A';

        summary += `    ${txnIndex + 1}. Date: ${transactionDate}, Amount: $${transactionAmount}, Description: ${transactionDescription}, Type: ${transactionType}, Customer: ${customerName}, Receipt: ${receiptUrl}\n`;
      });

      summary += `\n`;
    });

    // Send the email
    MailApp.sendEmail(emailAddress, subject, summary);
  };


  /**
   * Retrieves the current process ID from LOG_ID.
   * @returns {string} - The current process ID.
   * @throws Will throw an error if no process ID is available.
   */
  const getCurrentProcessId = () => {
    if (typeof LOG_ID !== 'undefined' && LOG_ID.length > 0) {
      return LOG_ID[LOG_ID.length - 1].id;
    } else {
      throw new Error('No process ID available.');
    }
  };

  /**
   * Helper function to sanitize filenames by replacing invalid characters.
   * @param {string} name - The filename to sanitize.
   * @returns {string} - The sanitized filename.
   */
  const sanitizeFileName = (name) => {
    return name.replace(/[<>:"\/\\|?*]+/g, '_'); // Replace invalid characters with underscore
  };

  /**
   * Saves the receipt blob to Google Drive, converting it to PDF if necessary.
   * @param {GoogleAppsScript.Base.Blob} blob - The blob content of the receipt.
   * @param {Object} transaction - The transaction object.
   * @param {string} mimeType - The MIME type of the receipt content.
   * @returns {Object} - An object containing the fileUrl and fileId of the saved receipt.
   */
  const saveReceiptToDrive = (blob, transaction, mimeType) => {
    // Use the folder URL from script properties or the default one if not set
    const folderUrl = sb_getProperty('RECEIPTS_FOLDER_URL') ||
      'https://drive.google.com/drive/folders/1TZLm4LmBWyOfWRaKIryl4uESlrEhSxRw';

    // Extract folder ID from the URL
    const folderIdMatch = folderUrl.match(/[-\w]{25,}/);  // Regex to extract folder ID
    if (!folderIdMatch) throw new Error('Invalid Google Drive folder URL.');

    const folderId = folderIdMatch[0];
    const folder = DriveApp.getFolderById(folderId);

    const formattedDate = new Date(transaction.created * 1000).toISOString().split('T')[0].replace(/-/g, '');
    let fileName = `${formattedDate}`;

    // Append customer name if it exists
    if (transaction.customer_name) {
      const sanitizedCustomerName = sanitizeFileName(transaction.customer_name);
      fileName += ` ${sanitizedCustomerName}`;
    }

    // Append transaction reporting category and description
    const sanitizedDescription = sanitizeFileName(`${transaction.reporting_category} ${transaction.description}`);
    fileName += ` ${sanitizedDescription}.pdf`;

    try {
      let fileUrl;
      let fileId;

      if (mimeType === 'text/html') {
        // Preprocess the HTML content to adjust tables and styles
        let htmlContent = blob.getDataAsString();

        // Remove HTML comments
        htmlContent = htmlContent.replace(/<!--[\s\S]*?-->/g, '');

        // Remove the <style> block entirely
        htmlContent = htmlContent.replace(/<style[\s\S]*?<\/style>/gi, '');

        // Remove unnecessary attributes (style, class, id) from all tags
        htmlContent = htmlContent.replace(/<(\w+)([^>]*)>/g, function(match, p1, p2) {
          // Remove style, class, id attributes
          let attrs = p2.replace(/\s+(style|class|id)="[^"]*"/gi, '');
          return `<${p1}${attrs}>`;
        });

        // Remove empty <div> and <span> tags
        htmlContent = htmlContent.replace(/<(div|span)[^>]*>\s*<\/\1>/gi, '');

        // Remove nested tables by replacing outer tables with their content
        htmlContent = htmlContent.replace(/<table[^>]*>\s*<tr[^>]*>\s*<td[^>]*>([\s\S]*?)<\/td>\s*<\/tr>\s*<\/table>/gi, '$1');

        // Remove width and height attributes from tables, tds, trs, and images
        htmlContent = htmlContent.replace(/(<(?:table|td|tr|th|img)[^>]*?)\s+(width|height)="[^"]*"/gi, '$1');
        htmlContent = htmlContent.replace(/(<(?:table|td|tr|th|img)[^>]*?)\s+(width|height):\s*[^;"]*;?/gi, '$1');

        // Add style="width:100%;" to all tables
        htmlContent = htmlContent.replace(/<table([^>]*)>/gi, '<table$1 style="width:100%;">');

        // Limit image widths to prevent them from exceeding page margins
        htmlContent = htmlContent.replace(/<img([^>]*)>/gi, '<img$1 style="max-width:100%;">');

        // Remove any remaining inline styles
        htmlContent = htmlContent.replace(/ style="[^"]*"/gi, '');

        // Remove any remaining empty tags
        htmlContent = htmlContent.replace(/<(\w+)[^>]*>\s*<\/\1>/gi, '');

        // Add the original Stripe URL link to the body of the HTML
        const originalReceiptUrl = transaction.receipt_url || '';
        if (originalReceiptUrl) {
          const linkHtml = `<p>Original receipt link: <a href="${originalReceiptUrl}">${originalReceiptUrl}</a></p>`;
          // Insert before </body> or at the end of the HTML content
          if (htmlContent.includes('</body>')) {
            htmlContent = htmlContent.replace('</body>', `${linkHtml}</body>`);
          } else {
            htmlContent += linkHtml;
          }
        }

        // Create a new HTML blob with the modified content
        const htmlBlob = Utilities.newBlob(htmlContent, MimeType.HTML, fileName);

        // Use Drive API to create a Google Doc from HTML without specifying parents
        const resource = {
          title: fileName,
          mimeType: MimeType.GOOGLE_DOCS,
        };
        const tempFile = Drive.Files.insert(resource, htmlBlob);

        // Since we cannot adjust page settings, proceed to export the PDF
        const pdfExportLink = tempFile.exportLinks['application/pdf'];

        // Fetch the PDF using OAuth token
        const pdfBlob = UrlFetchApp.fetch(pdfExportLink, {
          headers: {
            Authorization: `Bearer ${ScriptApp.getOAuthToken()}`
          }
        }).getBlob().setName(fileName);

        // Save the PDF to the desired folder
        const pdfFile = folder.createFile(pdfBlob);
        fileUrl = pdfFile.getUrl();
        fileId = pdfFile.getId();

        // Delete the temporary Google Doc
        Drive.Files.remove(tempFile.id);
      } else {
        // For other MIME types, save the file as is
        blob.setName(fileName);
        blob.setContentType(mimeType);

        const file = folder.createFile(blob);
        fileUrl = file.getUrl();
        fileId = file.getId();
      }

      return { fileUrl: fileUrl, fileId: fileId };  // Return the URL and ID of the saved file in Google Drive
    } catch (error) {
      // Include transaction object in the error message for debugging
      const debugInfo = `Transaction: ${JSON.stringify(transaction)}`;
      sidebar.log(`Debug Info: ${debugInfo}`);

      throw new Error(`Failed to save receipt to Google Drive for transaction: ${transaction.id}, Date: ${new Date(transaction.created * 1000)}. Error: ${error.message}`);
    }
  };

  /**
   * Creates a row array for a transaction to be inserted into the sheet.
   * @param {Array} baseRowData - The base data from the payout row.
   * @param {Object} transaction - The transaction object.
   * @param {Object} columnIndices - Object containing indices of required columns.
   * @param {string} originalPayoutDescription - The original payout description from the sheet.
   * @param {number} transactionAmount - The amount of the transaction.
   * @param {string|null} receiptDriveUrl - The URL of the saved receipt in Google Drive.
   * @returns {Array} - The row data array to be inserted into the sheet.
   */
const createTransactionRow = (baseRowData, transaction, columnIndices, originalPayoutDescription, transactionAmount, receiptDriveUrl, category, stripeAccountNumber) => {
  const rowData = [...baseRowData]; // Clone the base row data
  rowData[columnIndices.amount] = transactionAmount; // Set the transaction amount

  // Build the description
  let description = `${transaction.reporting_category}:` ; 

  // Append customer_name if it exists
  if (transaction.customer_name) {
    description += ` | Customer: ${transaction.customer_name}`;
  }

  description +=  ` | ${transaction.type}: ${transaction.description}`;

  // Append the original payout description
  description += ` | ${originalPayoutDescription}`;

  // Append source ID
  description += ` | Source: ${transaction.source}`;

  rowData[columnIndices.description] = description; // Set description
  rowData[columnIndices.date] = new Date(transaction.created * 1000); // Set the transaction date
  rowData[columnIndices.receiptUrl] = receiptDriveUrl || ''; // Set the receipt URL
  
  // Set new columns
  if (columnIndices.institution !== undefined) {
    rowData[columnIndices.institution] = STRIPE_INSTITUTION_NAME;
  }
  if (columnIndices.accountNumber !== undefined) {
    rowData[columnIndices.accountNumber] = stripeAccountNumber || '';
  }
  if (columnIndices.accountId !== undefined) {
    rowData[columnIndices.accountId] = ''; // Set to empty string as requested
  }
  if (columnIndices.category !== undefined) {
    rowData[columnIndices.category] = category;
  }

  return rowData;
};

  /**
   * Creates a row array for a transaction fee to be inserted into the sheet.
   * @param {Array} baseRowData - The base data from the payout row.
   * @param {Object} fee - The fee object from the transaction.
   * @param {Object} parentTransaction - The parent transaction object.
   * @param {Object} columnIndices - Object containing indices of required columns.
   * @param {string} originalPayoutDescription - The original payout description from the sheet.
   * @param {number} feeAmount - The amount of the fee.
   * @param {string|null} receiptDriveUrl - The URL of the saved receipt in Google Drive.
   * @returns {Array} - The row data array to be inserted into the sheet.
   */
  const createFeeRow = (baseRowData, fee, parentTransaction, columnIndices, originalPayoutDescription, feeAmount, receiptDriveUrl, category, stripeAccountNumber ) => {
    const rowData = [...baseRowData]; // Clone the base row data
    rowData[columnIndices.amount] = -feeAmount; // Set the fee amount as a negative value

    // Build the description
    let description = `${parentTransaction.reporting_category}:`;

    // Append customer_name if it exists in parentTransaction
    if (parentTransaction.customer_name) {
      description += ` | Customer: ${parentTransaction.customer_name}`;
    }

    description +=  ` | ${fee.type}}: ${fee.description}`;

    // Append the original payout description
    description += ` | ${originalPayoutDescription}`;

    // Append source ID from parent transaction
    description += ` | Source: ${parentTransaction.source}`;

    rowData[columnIndices.description]  = description; // Set description for the fee
    rowData[columnIndices.date]         = new Date(parentTransaction.created * 1000); // Use the parent transaction's date
    rowData[columnIndices.receiptUrl]   = receiptDriveUrl || ''; // Set the receipt URL
    rowData[columnIndices.category]     = STRIPE_FEE_CATEGORY_LABEL || category ;

    // Set institution and account details
    if (columnIndices.institution !== undefined) {
      rowData[columnIndices.institution] = STRIPE_INSTITUTION_NAME;
    }
    if (columnIndices.accountNumber !== undefined) {
      rowData[columnIndices.accountNumber] = stripeAccountNumber || '';
    }
    if (columnIndices.accountId !== undefined) {
      rowData[columnIndices.accountId] = ''; // Set to empty string as requested
    }

    if (columnIndices.transactionId !== undefined) {
      rowData[columnIndices.transactionId] = ''; // Fees may not have a Transaction ID
    }

    return rowData;
  };

  // Expose public functions
  return {
    processStripePayouts: processStripePayouts
  };

})();

// Global function that calls the namespaced version directly and manages LOG_ID
function tiller_tools_stripe_processStripePayouts(startDateInput = null, endDateInput = null) {
  //const retVal = sb_invokeWithId( "tiller_tools.stripe.processStripePayouts", "foo", startDateInput, endDateInput ) ;
  const retVal = tiller_tools.stripe.processStripePayouts( startDateInput, endDateInput ) ;

  return retVal ;
}