// Set this flag to true to enable logging, false to disable
var ENABLE_LOGGING = false;

/**
 * Expands date ranges with additional arguments appended to each row.
 * 
 * @param {(Array<Array<*>>|Array<*>)} dateRangesOrSingle - An array of date ranges or a single date range.
 * @param {boolean} [includeHeaders=false] - Flag to include headers in the output.
 * @returns {Array<Array<*>>} A 2D array where each row is an expanded array with dates and additional arguments.
 * @throws {Error} If the input arguments are invalid.
 */
function EXPAND_DATE_RANGE(dateRangesOrSingle, includeHeaders = false) {
  if (!Array.isArray(dateRangesOrSingle)) {
    throw new Error('The first argument must be an array.');
  }
  
  let result = [];
  let headers = [];
  let startDateCol = 0, countOfMonthsCol = 1;

  // Check if the first argument is an array of arrays or a single array
  if (Array.isArray(dateRangesOrSingle[0])) {
    if (isHeaderRow(dateRangesOrSingle[0])) {
      headers = dateRangesOrSingle.shift();
      startDateCol = headers.indexOf('StartDate');
      countOfMonthsCol = headers.indexOf('CountOfMonths');
      
      if (startDateCol === -1 || countOfMonthsCol === -1) {
        throw new Error('Headers must include StartDate and CountOfMonths.');
      }

      if (includeHeaders) {
        result.push(headers);
      }
    }

    // Validate and process each range in the array of arrays
    dateRangesOrSingle.forEach((range, index) => {
      if (!Array.isArray(range) || range.length < 2) {
        throw new Error(formatString('Each range must be an array with at least two elements: [date, countOfMonths, ...args]. Invalid range at index %s: %s', index, JSON.stringify(range)));
      }
      let startDate = range[startDateCol];
      let countOfMonths = range[countOfMonthsCol];
      let args = range.slice(0, startDateCol).concat(range.slice(startDateCol + 1, countOfMonthsCol)).concat(range.slice(countOfMonthsCol + 1));
      log(`Args for range at index ${index}: ${args.join(', ')}`);
      startDate = parseDate(startDate);
      if (!isValidDate(startDate)) {
        throw new Error(formatString('Invalid date at index %s: %s', index, startDate));
      }
      if (typeof countOfMonths !== 'number' || countOfMonths < 1) {
        throw new Error(formatString('Invalid countOfMonths at index %s: %s', index, countOfMonths));
      }
      processDateRange(startDate, countOfMonths, args, result);
    });
  } else {
    // Validate and process the single array
    if (!Array.isArray(dateRangesOrSingle) || dateRangesOrSingle.length < 2) {
      throw new Error('When passing a single array, it must have at least two elements: [StartDate, CountOfMonths, ...args].');
    }
    let startDate = dateRangesOrSingle[0];
    let countOfMonths = dateRangesOrSingle[1];
    let args = dateRangesOrSingle.slice(2);
    log(`Args for single range: ${args.join(', ')}`);
    startDate = parseDate(startDate);
    if (!isValidDate(startDate)) {
      throw new Error(formatString('Invalid date: %s', startDate));
    }
    if (typeof countOfMonths !== 'number' || countOfMonths < 1) {
      throw new Error(formatString('Invalid countOfMonths: %s', countOfMonths));
    }
    processDateRange(startDate, countOfMonths, args, result);
  }

  return result;
}

/**
 * Parses a date string or a Date object and returns a Date object set to 23:59 UTC.
 * Provides verbose error information if the date is invalid.
 * 
 * @param {string|Date} date - The date string or Date object.
 * @returns {Date} The parsed Date object set to 23:59 UTC.
 * @throws {Error} If the date format is invalid.
 */
function parseDate(date) {
  log(`Parsing date: ${date} of type ${typeof date}`);
  if (date instanceof Date) {
    return date;
  }
  if (typeof date === 'string') {
    let dateParts = date.split('/');
    if (dateParts.length === 3) {
      // Creating a date in ISO format with corrected month (0-based)
      let parsedDate = new Date(Date.UTC(dateParts[2], dateParts[0] - 1, dateParts[1], 23, 59, 0, 0));
      if (isNaN(parsedDate.getTime())) {
        throw new Error(`Unable to parse date: ${date}`);
      }
      return parsedDate;
    }
  }
  throw new Error(`Invalid date format. Expected date string in MM/DD/YYYY or Date object. Received: ${date}`);
}


/**
 * Processes a date range and appends rows to the result array.
 * 
 * @param {Date} startDate - The start date.
 * @param {number} countOfMonths - The number of months to expand.
 * @param {Array<*>} args - Additional arguments to append to each row.
 * @param {Array<Array<*>>} result - The result array to append rows to.
 */
function processDateRange(startDate, countOfMonths, args, result) {
  log(`Start date is ${startDate.toISOString()} of type ${typeof(startDate)}`);
  let year = startDate.getUTCFullYear();
  let month = startDate.getUTCMonth();
  let day = startDate.getUTCDate();

  for (let i = 0; i < countOfMonths; i++) {
    log(`Processing date: ${new Date(Date.UTC(year, month, day, 23, 59, 0, 0)).toISOString()} (originalDay: ${day})`);
    let newRow = [new Date(Date.UTC(year, month, day, 23, 59, 0, 0))].concat(args);
    result.push(newRow);

    // Move to the next month
    month += 1;

    // Adjust year if necessary
    if (month > 11) {
      month = 0;
      year += 1;
    }

    // If the new month has fewer days than the original day, set the date to the last day of the new month
    let daysInMonth = getDaysInMonth(year, month);
    if (day > daysInMonth) {
      day = daysInMonth;
    }
  }
  log(`Final date after processing: ${new Date(Date.UTC(year, month, day, 23, 59, 0, 0)).toISOString()}`);
}

/**
 * Formats a date as a string in the format YYYY-MM-DD.
 * 
 * @param {Date} date - The date to format.
 * @returns {string} The formatted date string.
 */
function formatDate(date) {
  const year = date.getUTCFullYear();
  const month = String(date.getUTCMonth() + 1).padStart(2, '0');
  const day = String(date.getUTCDate()).padStart(2, '0');
  return `${year}-${month}-${day}`;
}

/**
 * Checks if a date is valid.
 * 
 * @param {Date} date - The date to check.
 * @returns {boolean} True if the date is valid, false otherwise.
 */
function isValidDate(date) {
  return date instanceof Date && !isNaN(date.getTime());
}

/**
 * Gets the number of days in a specific month of a specific year.
 * 
 * @param {number} year - The year.
 * @param {number} month - The month (0-based, where January is 0 and December is 11).
 * @returns {number} The number of days in the month.
 */
function getDaysInMonth(year, month) {
  return new Date(Date.UTC(year, month + 1, 0)).getUTCDate();
}

/**
 * Custom function to expand date ranges with additional arguments.
 * 
 * @param {(Array<Array<*>>|Array<*>)} dateRangesOrSingle - An array of date ranges or a single date range.
 * @param {boolean} [includeHeaders=false] - Flag to include headers in the output.
 * @returns {Array<Array<*>>} A 2D array where each row is an expanded array with dates and additional arguments.
 * @throws {Error} If the input arguments are invalid.
 */
function SHEET_EXPAND_DATE_RANGE(dateRangesOrSingle, includeHeaders = false) {
  return EXPAND_DATE_RANGE(dateRangesOrSingle, includeHeaders);
}

/**
 * Simple string formatting function similar to printf in C.
 * 
 * @param {string} format - The format string containing %s placeholders.
 * @param {...*} args - The values to replace the placeholders with.
 * @returns {string} The formatted string.
 */
function formatString(format, ...args) {
  let i = 0;
  return format.replace(/%s/g, () => args[i++]);
}

/**
 * Logs a message if logging is enabled.
 * 
 * @param {string} message - The message to log.
 */
function log(message) {
  if (ENABLE_LOGGING) {
    Logger.log(message);
  }
}

/**
 * Checks if the first row is likely a header row.
 * 
 * @param {Array<*>} row - The first row of data.
 * @returns {boolean} True if the row is likely a header row, false otherwise.
 */
function isHeaderRow(row) {
  return row.includes('StartDate') && row.includes('CountOfMonths');
}

// Example usage:
function testExpandDateRange() {
  try {
    // Test with an array of arrays, including cases that cross year boundaries
    let dateRanges = [
      ["StartDate", "CountOfMonths", "Service", "FI", "TotalCost"],
      ["2/1/2024", 22, "GDOT/C2C FD", "Dev", "$79,950"],
      ["1/31/2024", 3, "Service", "FI", "$65,000", "123.00%", "100.00%", "$79,950"]
    ];
    
    let expandedData = EXPAND_DATE_RANGE(dateRanges, true);
    Logger.log(expandedData);

    // Test with a single array, including a case that crosses a year boundary
    let singleDateRange = ["11/30/2023", 14, "Service", "FI", "$65,000", "123.00%", "100.00%", "$79,950"];
    expandedData = EXPAND_DATE_RANGE(singleDateRange);
    Logger.log(expandedData);
  } catch (e) {
    Logger.log('Error: ' + e.message);
  }
}
