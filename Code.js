
const BASE_URL = "https://api.openai.com/v1/chat/completions";

function fetchData(systemContent, userContent, model = "gpt-3.5-turbo-16k") {

  const CHAT_GPT_API_KEY = null ;

  try {
    const headers = {
      "Content-Type": "application/json",
      "Authorization": `Bearer ${CHAT_GPT_API_KEY}`
    };

    const options = {
      headers,
      method: "GET",
      muteHttpExceptions: true,
      payload: JSON.stringify({
        // "model": "gpt-3.5-turbo",
        "model": model,
        "messages": [{
          "role": "system",
          "content": systemContent,
        },
        {
          "role": "user",
          "content": userContent
        },
        ],
        "temperature": 0.7
      })
    };

    const response = JSON.parse(UrlFetchApp.fetch(BASE_URL, options));
    
    if ( response.error && response.error.message )
      throw new Error("GPT Error: [%s] [%s]: %s: %s". response.error.type, response.error.code, response.error.message, userContent ) ;

    return response.choices[0].message.content;
  } catch (e) {
    logMessage(e)
    SpreadsheetApp.getActiveSpreadsheet().toast("Some Error Occured Please check your formula or try again later.");
    return "Some Error Occured Please check your formula or try again later.";
  }
}

/**
 * Simplifies the given paragraph in layman's term.
 * @param {String} input The value to simplify.
 * @return Simplified Text.
 * @customfunction
 */
function GPT_SIMPLIFY(input) {
  logMessage(input)
  const systemContent = "Simplify the given text in layman's term. Remember reader is not an expert in english.";
  return Array.isArray(input) ?
    input.flat().map(text => fetchData(systemContent, text)) :
    fetchData(systemContent, input);

}

const INVOICE = "Fieldwork Brewing - San Ramon - (925) 359-6961\
\
Check #100 for james Wiese\
Serving 3:47 PM PDT\
Fieldwork Brewing - San Ramon\
6000 Bollinger Canyon Road\
San Ramon, CA 94583\
 	\
Check #100	Table 45, james Wiese\
Guest Count: 1	\
Ordered:	9/29/23 3:47 PM\
 	\
1 Pretzel & Beer Cheese	10.00\
1 Melody & Silence	0.00\
 Full	8.00\
1 Castleford	0.00\
 Full	8.00\
 	\
Subtotal	26.00\
Tax	2.27\
Tip	4.68\
Total	32.95\
 	\
 	\
Amex	xxxxxxxx9828\
Time	9/29/2023, 6:16 PM\
 	\
Transaction Type	Sale\
Authorization	Approved\
Approval Code	\
Payment ID	fNMHq7YkgkTb\
Merchant ID	Merchant Id\
 	\
 	\
Stay in touch @\
www.FieldworkBrewing.com\
\
Cheers!" ;


/**
 * Summarzies the given paragraph. It provides from 3-5 bullet points
 *
 * @param {String} input The value to summarize.
 * @return summarize Text.
 * @customfunction
 */
function GPT_INVOICE(input, mimeType) {
  logMessage("Checking for invoice: %s", input);

  const systemContent = "You are a revenue operations data processor.  The following is either a regular email, or an invoice or a receipt which is OCR data form a a PDF, image or HTML file. If it's an invoice or receipt, parse it and return a JSON of {date, total, vendor, name, lineItems } with the date in ISO 8601 Date format (YYYY-MM-DDTHH:mm:ss-hh:mm), vendor, and total amount in USD for in the invoice do not return any extra text \
  in the response. lineItems are the individual line item charges in the format {quantity, description, subtotal} if no quantity is specified, assume it is 1. The vendor is the name of the vendor or company on the receipt.  The name should be in the format \"YYYYMMDD vendor.type\" where YYYY is the year, MM is the month of date found, DD is the day of the date found, vendor is the name of the vendor, and type is the MIME type but in extension format such as IMG or PDF or TEXT, etc. However, the content is not a receipt or invoice, return { total = null, date=null, vendor=\"\" }.  Make sure that either the success case or unsucessful case is returned in the precise format. If no time or timezone is found, assume 12:00:00 as the time, and the timezone as PDT. Extension for the mime typeof the name is " + mimeType + ": ";
  const toReturn = Array.isArray(input) ?
    input.flat().map(text => fetchData(systemContent, text)) :
    fetchData(systemContent, input);

  return JSON.parse( toReturn ) ;
}

/**
 * Summarzies the given paragraph. It provides from 3-5 bullet points
 *
 * @param {String} input The value to summarize.
 * @return summarize Text.
 * @customfunction
 */
function GPT_SUMMARY(input) {
  logMessage(input);
  const systemContent = "Summarize the given text. Provide atleast 3 and atmost 5 bullet points.";
  return Array.isArray(input) ?
    input.flat().map(text => fetchData(systemContent, text)) :
    fetchData(systemContent, input);

}

function TEST()
{
  GPT_INVOICE( INVOICE );
}
const DRIVE_NAME = "Fortified Strength Inc";

/**
 * Lists folders in the Fortified Strength Inc shared drive.
 * @returns {GoogleAppsScript.Drive.Folder[]} An array of folders.
 */
function getFoldersInSharedDrive() {
  const drives = Drive.Drives.list().items;
  const targetDrive = drives.find(drive => drive.name === DRIVE_NAME);
  
  if (!targetDrive) {
    throw new Error(`No shared drive found with name: ${DRIVE_NAME}`);
  }
  
  const driveId = targetDrive.id;
  const folderItems = Drive.Children.list(driveId, { q: "mimeType='application/vnd.google-apps.folder'" }).items;
  return folderItems.map(item => DriveApp.getFolderById(item.id));
}

function TEST_EMPTY_RECEIPTS()
{
  getEmptyReceiptRows().forEach( row => {
    logMessage( "Row: %s, Date: %s, Desc: %s, Total: %s", row.rowNumber, row.date, row.description, row.amount ) ;
  } ); 
}

function getEmptyReceiptRows(sheetName = 'Transactions', columnName = 'ReceiptURL') {
  const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(sheetName);
  const data = sheet.getDataRange().getValues();
  
  const [headers] = data;
  const receiptUrlColumnIndex = headers.indexOf(columnName);
  const dateColumnIndex = headers.indexOf('Date');
  const descriptionColumnIndex = headers.indexOf('Description');
  const categoryColumnIndex = headers.indexOf('Category');
  const amountColumnIndex = headers.indexOf('Amount');
  
  if ([receiptUrlColumnIndex, dateColumnIndex, descriptionColumnIndex, categoryColumnIndex, amountColumnIndex].includes(-1)) {
    throw new Error('One of the expected columns not found in the sheet.');
  }

  logMessage("Looking at sheet of %s rows", data.length ) ;

  let toReturn = data
    .slice(1)
    .map((row, index) => ({
      rowNumber: index + 2, // +2 because the headers are skipped and rows are 1-indexed in Sheets
      date: row[dateColumnIndex],
      description: row[descriptionColumnIndex],
      category: row[categoryColumnIndex],
      amount: row[amountColumnIndex],
      isEmptyReceipt: !row[receiptUrlColumnIndex]
    })) ;

  toReturn = 
    toReturn
    .filter(({ isEmptyReceipt }) => isEmptyReceipt) ;

  logMessage("Number of empty receipt rows: %s", toReturn.length ) ;
  
  toReturn = 
    toReturn
    .map(({ rowNumber, date, description, category, amount }) => ({
      rowNumber,
      date,
      description,
      category,
      amount
    }));

  return toReturn ;
}

function updateReceiptUrlForRow(rowNumber, newUrl, sheetName = 'Transactions', columnName = 'ReceiptURL') {
  const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(sheetName);
  const headers = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];
  const receiptUrlColumnIndex = headers.indexOf(columnName);
  
  if (receiptUrlColumnIndex === -1) {
    throw new Error('ReceiptURL column not found in the sheet.');
  }
  
  sheet.getRange(rowNumber, receiptUrlColumnIndex + 1).setValue(newUrl);
}

/**
 * Process matching emails, save attachments, and update the spreadsheet.
 * 
 * @param {Array<Object>} [emptyReceiptRows=getEmptyReceiptRows()] - Rows from the Transactions sheet with empty receipt URLs.
 * @param {string} [emailAddress='receipts@fortifiedstrength.org'] - Email address to filter the emails.
 * @param {number} [daysPrior=7] - Number of days prior to today to consider for the email search.
 * @param {string} [driveName='Fortified Strength Inc'] - Name of the Google Drive.
 * @param {string} [folderName='Receipts'] - Name of the folder inside the Drive where attachments will be saved.
 * @param {number} [varianceInDays=5] - Allowed variance in days for matching email and transaction dates.
 */
function processMatchingEmails(
  emailAddress    = "", // 'receipts@fortifiedstrength.org', 
  daysPrior       = 7,
  varianceInDays  = 5,
  driveName       = 'Fortified Strength Inc',
  folderName      = 'Receipts'
) {
  if (typeof driveName !== 'string' || !driveName) {
    throw new Error('Invalid driveName argument. It should be a non-empty string.');
  }

  if (typeof folderName !== 'string' || !folderName) {
    throw new Error('Invalid folderName argument. It should be a non-empty string.');
  }

  // if (!Array.isArray(emptyReceiptRows)) {
  //   throw new Error('Invalid emptyReceiptRows argument. It should be an array.');
  // }

  const driveId = Drive.Drives.list().items.find(drive => drive.name === driveName).id;
  if (!driveId) {
    throw new Error(`No shared drive found with name: ${driveName}`);
  }

  const folderQuery = `mimeType='application/vnd.google-apps.folder' and title='${folderName}' and '${driveId}' in parents`;
  const folders = Drive.Children.list(driveId, { q: folderQuery }).items;
  if (folders.length === 0) {
    throw new Error(`${folderName} folder not found in the ${driveName} drive.`);
  }
  const commonImageMimeTypes = [
      MimeType.JPEG, 
      MimeType.GIF, 
      MimeType.PNG
      // Note: MimeType doesn't provide constants for JPG or HEIC as of the last known update in January 2022
    ];

  const receiptsFolderId = folders[0].id;

  const timeBoundary = new Date();
  timeBoundary.setDate(timeBoundary.getDate() - daysPrior);
  let conditions = `after:${timeBoundary.toISOString().slice(0, 10)}`;
  
  // Update the filter conditions to exclude emails with empty "To" or "Cc" fields
  if (emailAddress) {
    conditions += ` {to:${emailAddress} cc:${emailAddress} bcc:${emailAddress}}`;
  }

  const emptyReceiptRows = getEmptyReceiptRows() ;

  logMessage("Searching for emails %s", conditions ) ;

  const threads = GmailApp.search(conditions);

  logMessage("Found %s emails", threads.length ) ;

  for (const thread of threads) {
    const messages = thread.getMessages();

    for (const message of messages) {
      logMessage(`Processing email: subject: ${message.getSubject()} on ${Utilities.formatDate(message.getDate(), Session.getScriptTimeZone(), 'yyyy-MM-dd')}`);

      const attachments = message.getAttachments();
      for (const attachment of attachments) {
        let content;
        const mimeType = attachment.getContentType();
        const extensionFromMimeType = mimeType.includes('/') ? mimeType.split('/').pop() : mimeType;

        if (mimeType === MimeType.PDF || commonImageMimeTypes.includes(mimeType) ) {
          const tempDoc = Drive.Files.insert({ title: 'tempOCRDoc', mimeType: MimeType.GOOGLE_DOCS }, attachment.copyBlob());
          content = DocumentApp.openById(tempDoc.id).getBody().getText();
          DriveApp.getFileById(tempDoc.id).setTrashed(true);
        } 
        else {
          content = attachment.getDataAsString();
        }

        if ( !content || content.trim().length == 0 )
          continue ;

        const { vendor, date: invoiceDate, total, name } = GPT_INVOICE(content, extensionFromMimeType);

        logMessage("GPT found vendor:%s, date:%s, total:%s, name:%s", vendor, invoiceDate, total, name ) ;

        if (total != null ) {
          logMessage("Examining %s empty receipt rows", emptyReceiptRows.length ) ;

          for (const { date, amount, rowNumber } of emptyReceiptRows) {
            const daysDifference = Math.abs((new Date(invoiceDate) - new Date(date)) / (24 * 60 * 60 * 1000));

            // logMessage("Transaction amount:%s comparing to total:%s and with %s days with %s variance of days allowed...", amount, total, daysDifference, varianceInDays ) ;

            if (daysDifference <= varianceInDays && (-1 * amount) == total) {
              logMessage("Matched transaction row with receipt row %s", rowNumber) ;

              const blob = attachment.copyBlob();
              const formattedDate = Utilities.formatDate(new Date(date), Session.getScriptTimeZone(), 'yyyyMMdd');
              const newName = name.replace(/^\d{8}/, formattedDate);
              blob.setName(newName);

              const file = DriveApp.getFolderById(receiptsFolderId).createFile(blob);              
              updateReceiptUrlForRow(rowNumber, file.getUrl());

              logMessage(`Match found for email from date: ${message.getDate()}, subject: ${message.getSubject()}`);
              logMessage(`Updating row: ${rowNumber}, date: ${date}, amount: ${amount}, ${newName} with URL: ${file.getUrl()}`);

              break; // Move to the next attachment once a match is found
            }
          }

          logMessage("Finished checking empty rows") ;
        }
        else
        {
          logMessage("GPT didn't find a valid total for this email");
        }
      }

      // Processing the mail body text
      try {
        const bodyContent = message.getPlainBody();
        const { vendor, date: invoiceDateFromBody, total: totalFromBody, name: nameFromBody } = GPT_INVOICE(bodyContent, "text");

        if (totalFromBody) {
          for (const { date, amount, rowNumber } of emptyReceiptRows) {
            const daysDifferenceFromBody = Math.abs((new Date(invoiceDateFromBody) - new Date(date)) / (24 * 60 * 60 * 1000));

            if (daysDifferenceFromBody <= varianceInDays && (-1 * amount) == totalFromBody) {
              logMessage(`Match found for email body from date: ${Utilities.formatDate(message.getDate(), Session.getScriptTimeZone(), 'yyyy-MM-dd')}, subject: ${message.getSubject()}`);
              logMessage(`Body content matched with row: ${rowNumber}`);

              // Create a file with the body content
              const blob = Utilities.newBlob(bodyContent, MimeType.PLAIN_TEXT, nameFromBody);
              const file = DriveApp.getFolderById(receiptsFolderId).createFile(blob);
              updateReceiptUrlForRow(rowNumber, file.getUrl());

              break; 
            }
          }
        }
      }
      catch( e )
      {
        logMessage("Error: an error occured while processing %s %s: %s", message.getDate(), message.getSubject(), e.message  ) ;
      }
    }
  }
}


// function processMatchingEmails(   emptyReceiptRows  = getEmptyReceiptRows(),
//                                   emailAddress      = 'receipts@fortifiedstrength.org', 
//                                   daysPrior         = 7,
//                                   driveName         = 'Fortified Strength Inc',
//                                   folderName        = 'Receipts',
//                                   varianceInDays    = 5
// ) 
// {
//   if (typeof driveName !== 'string' || !driveName) {
//     throw new Error('Invalid driveName argument. It should be a non-empty string.');
//   }

//   if (typeof folderName !== 'string' || !folderName) {
//     throw new Error('Invalid folderName argument. It should be a non-empty string.');
//   }
//   if (!Array.isArray(emptyReceiptRows)) {
//     throw new Error('Invalid emptyReceiptRows argument. It should be an array.');
//   }
//   const driveId = Drive.Drives.list().items.find(drive => drive.name === driveName).id;
//   if (!driveId) {
//     throw new Error(`No shared drive found with name: ${driveName}`);
//   }
  
//   const folderQuery = `mimeType='application/vnd.google-apps.folder' and title='${folderName}' and '${driveId}' in parents`;
//   const folders = Drive.Children.list(driveId, { q: folderQuery }).items;
//   if (folders.length === 0) {
//     throw new Error(`${folderName} folder not found in the ${driveName} drive.`);
//   }
//   const receiptsFolderId = folders[0].id;
  
//   const timeBoundary = new Date();
//   timeBoundary.setDate(timeBoundary.getDate() - daysPrior);
//   const threads = GmailApp.search(`after:${timeBoundary.toISOString().slice(0, 10)} {to:${emailAddress} cc:${emailAddress} bcc:${emailAddress}}`);
  
//   for (const thread of threads) {
//     const messages = thread.getMessages();
    
//     for (const message of messages) {
//       const attachments = message.getAttachments();
//       for (const attachment of attachments) {
//         let content;
//         const mimeType              = attachment.getContentType();
//         const extensionFromMimeType = mimeType.includes('/') ? mimeType.split('/').pop() : mimeType;


//         if (attachment.getContentType() === MimeType.PDF) {
//           const tempDoc = Drive.Files.insert({ title: 'tempOCRDoc', mimeType: MimeType.GOOGLE_DOCS }, attachment.copyBlob());
//           content = DocumentApp.openById(tempDoc.id).getBody().getText();
//           DriveApp.getFileById(tempDoc.id).setTrashed(true);
//         } else {
//           content = attachment.getDataAsString(MimeType.PLAIN_TEXT);
//         }
        
//         const { vendor, date: invoiceDate, total, name } = GPT_INVOICE(content, extensionFromMimeType);

//         if (total) {
//           for (const { date, amount, rowNumber } of emptyReceiptRows) {
//             const daysDifference = Math.abs((new Date(invoiceDate) - new Date(date)) / (24 * 60 * 60 * 1000));

//             if (daysDifference <= varianceInDays && (-1 * amount) == total) {
//               const blob = attachment.copyBlob(); // Make a copy of the original blob

//               const formattedDate = Utilities.formatDate(new Date(date), Session.getScriptTimeZone(), 'yyyyMMdd');
//               const newName = name.replace(/^\d{8}/, formattedDate); // Replace the first 8 digits (YYYYMMDD) with the new date
//               blob.setName(newName); // Set the updated name


//               const file = DriveApp.getFolderById(receiptsFolderId).createFile(blob);
//               updateReceiptUrlForRow(rowNumber, file.getUrl());
//               break;  // Move to the next attachment once a match is found
//             }
//           }
//         }
//       }
//     }
//   }
// }

// /**
//  * @throws Will throw an error if the necessary folders are not found or if there's a problem reading a file.
//  */
// function walkDriveAndProcessInvoices() 
// {
//   const folders = getFoldersInSharedDrive();
//   const receiptsFolder = folders.find(folder => folder.getName() === 'Receipts');
//   if (!receiptsFolder) throw new Error('Expected "Receipts" folder not found in shared drive.');
  
//   if (!receiptsFolder) throw new Error('Expected "Receipts" sub-folder to exist inside "Fortified Strength" but it was not found.');

//   const files = receiptsFolder.getFiles();
//   const matchedRows = [];

//   while (files.hasNext()) {
//     const file = files.next();
//     let content;

//     if (['image/png', 'image/jpeg', 'application/pdf'].includes(file.getMimeType())) {
//       const tempFile = file.makeCopy(DriveApp.getRootFolder());
//       const thumbnail = tempFile.getThumbnail();

//       try {
//         // content = file.getAs(MimeType.PLAIN_TEXT).getDataAsString();
//         content = DocumentApp.openById(tempFile.getId()).getBody().getText();

//       } catch (e) {
//         throw new Error(`Expected to extract content from file "${file.getName()}" but encountered error: ${e.message}`);
//       }
      
//       tempFile.setTrashed(true);
//     } else {
//       content = file.getAs('text/plain').getDataAsString() ;
//     }
    
//     const { vendor, date, total } = GPT_INVOICE(content);
//     const matchedIndices = matchInSpreadsheet(date, total);
//     matchedRows.push(...matchedIndices);
//   }

//   return matchedRows;
// }
function getFoldersInSharedDrive() {
  
  const drives = Drive.Drives.list().items;
  const targetDrive = drives.find(drive => drive.name === DRIVE_NAME);
  
  if (!targetDrive) {
    throw new Error(`No shared drive found with name: ${DRIVE_NAME}`);
  }
  
  const driveId = targetDrive.id;
  const folderItems = Drive.Children.list(driveId, { q: "mimeType='application/vnd.google-apps.folder'" }).items;
  return folderItems.map(item => DriveApp.getFolderById(item.id));
}

function walkDriveAndProcessInvoices() {
  const folders = getFoldersInSharedDrive();
  const receiptsFolder = folders.find(folder => folder.getName() === 'Receipts');
  
  if (!receiptsFolder) throw new Error('Expected "Receipts" folder not found in shared drive.');
  
  const files = receiptsFolder.getFiles();
  const matchedRows = [];

  while (files.hasNext()) {
    const file = files.next();
    let content;

    if (file.getMimeType() === MimeType.PDF) {
      const tempDoc = Drive.Files.insert({ title: 'tempOCRDoc', mimeType: MimeType.GOOGLE_DOCS }, file.getBlob());
      DriveApp.getFileById(tempDoc.id).setTrashed(true);

      content = DocumentApp.openById(tempDoc.id).getBody().getText();
      
    } else {
      content = file.getAs(MimeType.PLAIN_TEXT).getDataAsString();
    }

    const { vendor, date, total } = GPT_INVOICE(content);
    const matchedIndices = matchInSpreadsheet(date, total);
    matchedRows.push(...matchedIndices);
  }

  return matchedRows;
}

/**
 * @throws Will throw an error if inputs are invalid or if the "transactions" tab is not found.
 */
function matchInSpreadsheet(date, total)
{
  if (!date || typeof date !== 'string' || typeof total !== 'number') {
    throw new Error(`Expected a valid date (string) and total (number) for matchInSpreadsheet. Received date: ${date}, total: ${total}`);
  }

  const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName('transactions');
  if (!sheet) throw new Error('Expected a "transactions" tab in the current spreadsheet but it was not found.');

  const data = sheet.getDataRange().getValues();
  const dateColIndex = data[0].findIndex(header => header.toLowerCase() === 'date');
  const amountColIndex = data[0].findIndex(header => header.toLowerCase() === 'amount');
  
  const matchedIndices = [];

  data.forEach((row, index) => {
    if (row[dateColIndex] === date && parseFloat(row[amountColIndex]) === total) {
      matchedIndices.push(index);
    }
  });

  return matchedIndices;
}
