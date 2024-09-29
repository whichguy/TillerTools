// Constants
const STRIPE_PAYOUT_DESCRIPTION_PREFIX = PropertiesService.getScriptProperties().getProperty('STRIPE_PAYOUT_DESCRIPTION_PREFIX') || "Orig Co Name:stripe Orig ID:x8598";

// Initialize logging variables
let logEntries = [];      // Array to store log messages
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
 * @param {string|null} startDateStr - Start date in 'YYYY-MM-DD' format or null for no limit.
 * @param {string|null} endDateStr - End date in 'YYYY-MM-DD' format or null for no limit.
 */
const processStripePayouts = (startDateStr = null, endDateStr = null) => {
  const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName("Transactions");
  if (!sheet) throw new Error("Sheet 'Transactions' not found. Please check the sheet name.");

  const headers = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];

  try {
    logEntries.push("Starting to process Stripe payouts.");

    // Ensure all required columns exist
    const dateColumnIndex = findColumnIndex(headers, "Date");
    const descriptionColumnIndex = findColumnIndex(headers, "Description");
    const amountColumnIndex = findColumnIndex(headers, "Amount");
    const receiptUrlColumnIndex = findColumnIndex(headers, "ReceiptURL");

    const columnIndices = {
      date: dateColumnIndex,
      description: descriptionColumnIndex,
      amount: amountColumnIndex,
      receiptUrl: receiptUrlColumnIndex
    };

    logEntries.push(`Found required columns: Date (${dateColumnIndex}), Description (${descriptionColumnIndex}), Amount (${amountColumnIndex}), ReceiptURL (${receiptUrlColumnIndex}).`);

    // Parse start and end dates
    logEntries.push(`Parsing date range with startDateStr: ${startDateStr}, endDateStr: ${endDateStr}`);
    const { startDate, endDate } = parseDateRange(startDateStr, endDateStr);

    logEntries.push(`Processing payouts from ${startDate ? startDate.toISOString().split('T')[0] : 'beginning'} to ${endDate ? endDate.toISOString().split('T')[0] : 'present'}.`);

    // Filter payout rows from the sheet within the date range
    const payoutRows = filterStripePayoutRows(sheet, dateColumnIndex, descriptionColumnIndex, amountColumnIndex, startDate, endDate);
    logEntries.push(`Found ${payoutRows.length} payout rows in the sheet within the date range.`);

    // Fetch payouts from Stripe within the date range
    const stripePayouts = fetchStripePayouts(startDate, endDate);
    logEntries.push(`Fetched ${stripePayouts.length} payouts from Stripe within the date range.`);

    // Map to store payoutId to payoutRow for easy access
    const payoutRowMap = new Map();

    // Prepare for batch fetching transactions
    let payoutIdsToProcess = [];

    // Find matching payouts and collect payout IDs
    payoutRows.forEach(payoutRow => {
      const matchingPayout = findMatchingPayout(payoutRow, stripePayouts);
      if (matchingPayout) {
        logEntries.push(`Found matching payout ID: ${matchingPayout.id} for row ${payoutRow.rowIndex}`);
        payoutRowMap.set(matchingPayout.id, {
          payoutRow: payoutRow,
          matchingPayout: matchingPayout
        });
        payoutIdsToProcess.push(matchingPayout.id);
      } else {
        logEntries.push(`No matching payout found for row ${payoutRow.rowIndex}, Date: ${payoutRow.date}, Description: ${payoutRow.description}`);
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
      } else {
        logEntries.push(`No transactions found for payout ID: ${payoutId}`);
      }
    });

  } catch (error) {
    // Enrich error messages with context
    logEntries.push(`Error in processStripePayouts: ${error.message}`);
    Logger.log(`Error in processStripePayouts: ${error.message}`);
    SpreadsheetApp.getUi().alert(`Error processing Stripe payouts: ${error.message}`);
  } finally {
    // Send summary email with logs and errors, even if an exception occurred
    compileAndSendSummaryEmail();
  }
};

/**
 * Parses the start and end date strings into Date objects.
 * If null, there is no limit on the respective date.
 * @param {string|null} startDateStr - Start date string in 'YYYY-MM-DD' format or null.
 * @param {string|null} endDateStr - End date string in 'YYYY-MM-DD' format or null.
 * @returns {Object} - Object containing startDate and endDate as Date objects or null.
 */
const parseDateRange = (startDateStr, endDateStr) => {
  logEntries.push("Starting parseDateRange function.");
  let startDate = null;
  let endDate = null;

  // Parse start date
  if (startDateStr) {
    startDate = new Date(startDateStr);
    if (isNaN(startDate.getTime())) {
      throw new Error(`Invalid start date format: ${startDateStr}`);
    }
    logEntries.push(`Parsed startDate: ${startDate.toISOString()}`);
  } else {
    logEntries.push("No startDateStr provided; startDate is null (no lower limit).");
  }

  // Parse end date
  if (endDateStr) {
    endDate = new Date(endDateStr);
    if (isNaN(endDate.getTime())) {
      throw new Error(`Invalid end date format: ${endDateStr}`);
    }
    logEntries.push(`Parsed endDate: ${endDate.toISOString()}`);
  } else {
    logEntries.push("No endDateStr provided; endDate is null (no upper limit).");
  }

  return { startDate, endDate };
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
  const apiKey = PropertiesService.getScriptProperties().getProperty('STRIPE_API_KEY');
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

      logEntries.push(`Fetched ${result.data.length} payouts from URL: ${url}`);

      if (result.has_more) {
        const lastPayoutId = result.data[result.data.length - 1].id;
        url = `https://api.stripe.com/v1/payouts?limit=100&starting_after=${lastPayoutId}${dateParams}`;
        urlsAccessed.push(url);
      } else {
        hasMore = false;
      }
    }

    logEntries.push(`Total payouts fetched: ${allPayouts.length}`);
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
  const apiKey = PropertiesService.getScriptProperties().getProperty('STRIPE_API_KEY');
  if (!apiKey) throw new Error("Stripe API key not found in script properties.");

  const payoutsTransactionsMap = {};

  // Prepare initial requests for all payouts
  let requestInfos = payoutIds.map(payoutId => {
    const url = `https://api.stripe.com/v1/balance_transactions?limit=100&payout=${payoutId}`;
    urlsAccessed.push(url);

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
  if (!sheet || !payoutRow || !payout || !headers || !columnIndices || !transactions) {
    throw new Error("Invalid arguments provided to processPayoutTransactions.");
  }

  const descriptionColumnIndex = columnIndices.description;
  const payoutRowData = sheet.getRange(payoutRow.rowIndex, 1, 1, headers.length).getValues()[0];
  const originalPayoutDescription = payoutRowData[descriptionColumnIndex - 1];

  const currentColumnIndices = {
    amount: columnIndices.amount - 1,
    description: columnIndices.description - 1,
    date: columnIndices.date - 1,
    receiptUrl: columnIndices.receiptUrl - 1,
  };

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
        logEntries.push(`Transaction ${transaction.id} has null source. Using payout ID ${payout.id} as source.`);
      }

      if (transaction.source.startsWith('ch_')) {
        // Proceed to collect charge IDs
        chargeIds.push(transaction.source);
        transactionChargeMap[transaction.id] = transaction.source;
      } else {
        // For other types of sources, proceed accordingly
        logEntries.push(`Transaction ${transaction.id} has non-charge source ${transaction.source}. Proceeding without charge data.`);
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
        logEntries.push(`Charge data not available for transaction ${transaction.id}. Proceeding without charge data.`);
        transaction.customer_name = null;
        transaction.receipt_url = null;
      }

      // Store transaction data for row creation
      // transaction.receipt_url is already set
    }

    // Fetch all receipts in parallel
    const receiptFilesMap = fetchAndSaveReceipts(receiptUrls, transactions, filesCreated);

    // Now create rows for transactions
    for (let transaction of transactions) {
      if (transaction.status !== 'available') {
        continue; // Skip transactions that are not available
      }

      const transactionAmount = transaction.amount / 100 || 0; // Set amount to zero if undefined

      // Get saved receipt file URL
      let receiptDriveUrl = null;
      if (transaction.receipt_url && receiptFilesMap[transaction.receipt_url]) {
        receiptDriveUrl = receiptFilesMap[transaction.receipt_url];
      }

      // Create transaction row
      const transactionRow = createTransactionRow(
        payoutRowData,
        transaction,
        currentColumnIndices,
        originalPayoutDescription,
        transactionAmount,
        receiptDriveUrl
      );
      newRowsData.push(transactionRow);

      // Log the row added
      rowsModified.push({
        rowIndex: payoutRow.rowIndex + newRowsData.length,
        transactionId: transaction.id,
        description: transactionRow[currentColumnIndices.description],
        amount: transactionRow[currentColumnIndices.amount],
        date: transactionRow[currentColumnIndices.date],
      });

      if (transaction.fee_details && transaction.fee_details.length > 0) {
        for (let fee of transaction.fee_details) {
          const feeAmount = fee.amount / 100 || 0;
          const feeRow = createFeeRow(
            payoutRowData,
            fee,
            transaction,
            currentColumnIndices,
            originalPayoutDescription,
            feeAmount,
            receiptDriveUrl
          );
          newRowsData.push(feeRow);

          // Log the fee row added
          rowsModified.push({
            rowIndex: payoutRow.rowIndex + newRowsData.length,
            transactionId: fee.type,
            description: feeRow[currentColumnIndices.description],
            amount: feeRow[currentColumnIndices.amount],
            date: feeRow[currentColumnIndices.date],
          });
        }
      }
    }

    // Calculate the total processed amount by summing amounts from newRowsData
    let totalProcessedAmount = newRowsData.reduce((sum, row) => {
      const amount = row[currentColumnIndices.amount];
      return sum + (typeof amount === 'number' ? amount : 0);
    }, 0);

    // Validate that the total processed amount matches the payout amount
    if (Math.abs(totalProcessedAmount - payoutRow.amount) > 0.01) {
      const errorMessage = `Total processed amount (${totalProcessedAmount.toFixed(2)}) does not match payout amount (${payoutRow.amount.toFixed(2)}). Transactions will not be inserted.`;
      logEntries.push(errorMessage);
      Logger.log(errorMessage);

      // Delete any files created during processing
      deleteFiles(filesCreated);

      throw new Error(`Transaction processing failed for payout on Date: ${payoutRow.date}, Description: ${payoutRow.description}. Total amount mismatch.`);
    } else {
      // Insert new rows and update data
      if (newRowsData.length > 0) {
        sheet.insertRowsAfter(payoutRow.rowIndex, newRowsData.length);
        sheet.getRange(payoutRow.rowIndex + 1, 1, newRowsData.length, headers.length).setValues(newRowsData);
        logEntries.push(`Inserted ${newRowsData.length} new rows after row ${payoutRow.rowIndex}`);
      }

      // Update the payout row's Amount to zero
      sheet.getRange(payoutRow.rowIndex, columnIndices.amount).setValue(0);
      logEntries.push(`Set payout row amount to zero for row ${payoutRow.rowIndex}`);

      logEntries.push(`Total processed amount matches the payout amount.`);
    }

  } catch (error) {
    const errorMessage = `Error processing payout for row ${payoutRow.rowIndex}: ${error.message}`;
    logEntries.push(errorMessage);
    Logger.log(errorMessage);

    // Delete any files created during processing
    deleteFiles(filesCreated);

    // Rollback any changes
    rollbackChanges(sheet, payoutRow.rowIndex, newRowsData.length, originalData);

    throw new Error(`Transaction processing failed for Date: ${payoutRow.date}, Description: ${payoutRow.description}. Error: ${error.message}`);
  }
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
        logEntries.push(`Failed to fetch charge ${chargeId}: ${response.getContentText()}`);
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
        logEntries.push(`Saved receipt for transaction ID: ${transaction.id}, File URL: ${receiptDriveUrl}`);

      } else {
        logEntries.push(`Failed to fetch receipt from URL: ${receiptUrl}, Response Code: ${response.getResponseCode()}`);
      }
    });

    return receiptFilesMap;

  } catch (error) {
    logEntries.push(`Error fetching and saving receipts in batch: ${error.message}`);
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
      logEntries.push(`Deleted file with ID: ${fileId}`);
    } catch (err) {
      logEntries.push(`Failed to delete file with ID: ${fileId}. Error: ${err.message}`);
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
      logEntries.push(`Rolled back: Deleted ${newRowsCount} new rows starting from row ${payoutRowIndex + 1}.`);
    }

    // Restore original data starting from payoutRowIndex
    sheet.getRange(payoutRowIndex, 1, originalData.length, originalData[0].length).setValues(originalData);
    logEntries.push(`Rolled back: Restored original data starting from row ${payoutRowIndex}.`);

  } catch (error) {
    throw new Error(`Rollback failed for row ${payoutRowIndex}. Error: ${error.message}`);
  }
};

/**
 * Compiles and sends a summary email with the log entries and any errors.
 */
const compileAndSendSummaryEmail = () => {
  // Fetch the email address from script properties, defaulting to finance@fortifiedstrength.org
  const emailAddress = PropertiesService.getScriptProperties().getProperty('SUMMARY_EMAIL') || 'finance@fortifiedstrength.org';

  const subject = 'Stripe Updates to Transactions';

  // Compile the summary
  let summary = 'Summary of changes:\n\n';

  // Include number of rows modified
  summary += `Rows modified or added: ${rowsModified.length}\n`;
  summary += `Files added: ${filesAdded.length}\n`;
  summary += `URLs accessed: ${urlsAccessed.length}\n\n`;

  // Include details
  summary += 'Detailed Log Entries:\n';
  logEntries.forEach(entry => {
    summary += `- ${entry}\n`;
  });

  // Send the email
  MailApp.sendEmail(emailAddress, subject, summary);
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
  const folderUrl = PropertiesService.getScriptProperties().getProperty('RECEIPTS_FOLDER_URL') ||
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
    logEntries.push(`Debug Info: ${debugInfo}`);

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
const createTransactionRow = (baseRowData, transaction, columnIndices, originalPayoutDescription, transactionAmount, receiptDriveUrl) => {
  const rowData = [...baseRowData]; // Clone the base row data
  rowData[columnIndices.amount] = transactionAmount; // Set the transaction amount

  // Build the description
  let description = `${transaction.reporting_category}: ${transaction.description} (${transaction.type})`;

  // Append source ID
  description += ` | Source: ${transaction.source}`;

  // Append customer_name if it exists
  if (transaction.customer_name) {
    description += ` | Customer: ${transaction.customer_name}`;
  }

  // Append the original payout description
  description += ` | ${originalPayoutDescription}`;

  rowData[columnIndices.description] = description; // Set description
  rowData[columnIndices.date] = new Date(transaction.created * 1000); // Set the transaction date
  rowData[columnIndices.receiptUrl] = receiptDriveUrl || ''; // Set the receipt URL

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
const createFeeRow = (baseRowData, fee, parentTransaction, columnIndices, originalPayoutDescription, feeAmount, receiptDriveUrl) => {
  const rowData = [...baseRowData]; // Clone the base row data
  rowData[columnIndices.amount] = -feeAmount; // Set the fee amount as a negative value

  // Build the description
  let description = `${parentTransaction.reporting_category}: ${fee.description} (${fee.type})`;

  // Append source ID from parent transaction
  description += ` | Source: ${parentTransaction.source}`;

  // Append customer_name if it exists in parentTransaction
  if (parentTransaction.customer_name) {
    description += ` | Customer: ${parentTransaction.customer_name}`;
  }

  // Append the original payout description
  description += ` | ${originalPayoutDescription}`;

  rowData[columnIndices.description] = description; // Set description for the fee
  rowData[columnIndices.date] = new Date(parentTransaction.created * 1000); // Use the parent transaction's date
  rowData[columnIndices.receiptUrl] = receiptDriveUrl || ''; // Set the receipt URL

  return rowData;
};

/**
 * Opens the configuration dialog for setting script properties.
 */
const openConfigDialog = () => {
  const html = HtmlService.createHtmlOutputFromFile('Config')
    .setWidth(400)
    .setHeight(400);
  SpreadsheetApp.getUi().showModalDialog(html, 'Configure Stripe Settings');
};

/**
 * Saves the configuration settings from the dialog.
 * @param {Object} config - Configuration object containing API key, folder URL, etc.
 * @returns {string} - Success message.
 */
const saveConfig = (config) => {
  PropertiesService.getScriptProperties().setProperties({
    STRIPE_API_KEY: config.apiKey,
    RECEIPTS_FOLDER_URL: config.receiptsFolderUrl,
    STRIPE_PAYOUT_DESCRIPTION_PREFIX: config.payoutDescriptionPrefix,
    SUMMARY_EMAIL: config.summaryEmail
  });
  return 'Configuration saved successfully!';
};

/**
 * Retrieves the current configuration settings.
 * @returns {Object} - Configuration object containing API key, folder URL, etc.
 */
const getConfig = () => {
  const properties = PropertiesService.getScriptProperties();
  return {
    apiKey: properties.getProperty('STRIPE_API_KEY') || '',
    receiptsFolderUrl: properties.getProperty('RECEIPTS_FOLDER_URL') || '',
    payoutDescriptionPrefix: properties.getProperty('STRIPE_PAYOUT_DESCRIPTION_PREFIX') || '',
    summaryEmail: properties.getProperty('SUMMARY_EMAIL') || ''
  };
};
