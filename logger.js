var LOG_ID = [];

function invokeWithId(functionName, functionId, ...args) 
{    
  console.log("Starting Function Name:", functionName);
  console.log("Process ID:", functionId);
  console.log("Arguments:", JSON.stringify(args) );

  const functionIdAndName = functionName + "_" + functionId ;

    const f = function(id, name) {
        return this[functionName](...args);
    };
    
    LOG_ID.push( {id: functionId, name: functionName } ) ;

    this[functionIdAndName] = f ; // create the new function in this namespace
    
    let result;
    try {
        result = this[functionIdAndName](functionId, functionName); // invoke the new function
    } finally {
        delete this[functionIdAndName]; // clean up and remove this function 

        LOG_ID.pop() ; // remove this off the stack 
    }

    return result;
}

function clearCache() {
  const cache = CacheService.getScriptCache();
  cache.removeAll();
}

function include(filename) 
{
  return HtmlService.createHtmlOutputFromFile(filename)
    .getContent()
    .replaceAll("&lt;" ,"<")
    .replaceAll("&gt;" ,">") ;
}

/**
 * Simulates a long-running process with interleaved short and long messages.
 * @param {string} arg1 - First argument.
 * @param {string} arg2 - Second argument.
 */
function longRunningFunction(arg1, arg2) {

    // Debug log the stack trace when invoking a function
    try {
        throw new Error("Logging stack trace for debugging:");
    } catch (e) {
        console.log(e.stack);
    }

    logMessage("Starting the process...");
    Utilities.sleep(400); // Sleep for 2 seconds

    logMessage("Part way through the process...");
    Utilities.sleep(200); // Sleep for 1 second

    for(let i = 0; i < 5; i++) { // Loop 5 times for testing
        logMessage(`Looping iteration ${i + 1}...`);
        Utilities.sleep(100); // Sleep for 0.5 seconds
    }

    // Insert a very long message
    logMessage("This is a very long message intended to test the collapsible functionality in the UI. It should be truncated with an ellipsis and provide an option to expand and view the full content. The message continues with more details and information to ensure that it exceeds the typical width of the log display area.");

    Utilities.sleep(2000); // Sleep for 2 seconds

    logMessage("Finished the process...");
}

function logMessage(message) {
  
    const id = extractIdFromFunctionName();
    console.log("logMessage: [%s]: %s", id, message );

    const lock = LockService.getScriptLock();
    try {
        lock.waitLock(10000); // Wait for up to 10 seconds

        const cache = CacheService.getDocumentCache();
        const existingJson = cache.get(id) || "[]" ;
        const existingMessages = JSON.parse(existingJson) ;
        existingMessages.push(message) ;
        cache.put(id, JSON.stringify(existingMessages), 150);
    } catch (e) {
        console.error("Error with logMessage:", e.toString());
    } finally {
        lock.releaseLock();
    }

    console.log("Finished logMessage function.");
}

function extractIdFromFunctionName() {
  Logger.log("Log ID: %s", LOG_ID[0].id ) ;
  return LOG_ID[0].id ;
}

function getProcessStatus(id) {
    // console.log("Starting getProcessStatus function...");

    const lock = LockService.getScriptLock();
    try {
        // console.log("Attempting to acquire lock...");
        lock.waitLock(10000); // Wait for up to 10 seconds
        // console.log("Lock acquired.");

        const cache = CacheService.getDocumentCache();
        const messages = cache.get(id) ? JSON.parse(cache.get(id)) : [];
        console.log("Retrieved messages from cache for ID", id, ":", messages);

        // Clear the messages after they've been fetched
        cache.remove(id);
        // console.log("Removed messages from cache for ID:", id);
        
        return {
            messages: messages
        };
    } catch (e) {
        console.error("Error with getProcessStatus:", e.toString());
        return { messages: [] };
    } finally {
        lock.releaseLock();
        console.log("Released the lock.");
    }

    // console.log("Finished getProcessStatus function.");
}



/**
 * Retrieves configuration properties from the script properties.
 *
 * @returns {Array} - An array of configuration objects.
 */
function getConfigProperties() {
    const properties = PropertiesService.getScriptProperties().getProperties();
    // Transform properties into a structured array
    // Assuming properties are stored as key-value pairs with type prefixes, e.g., "date_startDate", "text_apiKey"
    const configArray = [];

    for (const key in properties) {
        let type = 'text'; // Default type
        let label = key;
        let value = properties[key];
        let options = [];

        const underscoreIndex = key.indexOf('_');

        if (underscoreIndex > 0) {
            const possibleType = key.substring(0, underscoreIndex).toLowerCase();
            const possibleLabel = key.substring(underscoreIndex + 1);

            // Recognized data types
            const recognizedTypes = ['date', 'number', 'select', 'textarea'];

            if (recognizedTypes.includes(possibleType)) {
                type = possibleType;
                // Convert camelCase or snake_case to Title Case with spaces
                label = possibleLabel.replace(/([A-Z])/g, ' $1').replace(/_/g, ' ').replace(/^./, function(str){ return str.toUpperCase(); });
            }
        }

        // Example options for select type
        if (type === 'select') {
            options = ['Option 1', 'Option 2', 'Option 3']; // Replace with actual options or fetch dynamically
        }

        configArray.push({
            name: key,
            label: label,
            type: type,
            value: value,
            options: options
        });
    }

    return configArray;
}

/**
 * Saves configuration properties sent from the client.
 *
 * @param {Array} configData - Array of objects containing key and value.
 * @returns {string} - Confirmation message.
 */
function saveConfigProperties(configData) {
    configData.forEach(function(item) {
        PropertiesService.getScriptProperties().setProperty(item.key, item.value);
    });
    return "Configurations saved successfully.";
}

