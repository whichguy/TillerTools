var LOG_ID = [];

/**
 * Invokes a function by its name and function ID while ensuring the function runs within the global scope.
 * @param {string} functionName - The name of the function to invoke.
 * @param {string} functionId - A unique ID for the function instance.
 * @param {...*} args - The arguments to pass to the invoked function.
 * @returns {*} - The result of the invoked function.
 */
function sb_invokeWithId(functionName, functionId, ...args) {
    console.log("Starting Function Name:", functionName);
    console.log("Process ID:", functionId);
    console.log("Arguments:", JSON.stringify(args));

    const functionIdAndName = functionName + "_" + functionId;

    const f = function (_functionId, _functionName) {
        return this[functionName](...args);
    };

    LOG_ID.push({ id: functionId, name: functionName });

    this[functionIdAndName] = f; // Create the new function in this namespace

    let result;
    try {
        result = this[functionIdAndName](functionId, functionName); // Invoke the new function
    } finally {
        delete this[functionIdAndName]; // Clean up and remove this function
        LOG_ID.pop(); // Remove this off the stack
    }

    return result;
}

/**
 * Stores input arguments in user properties with the function name as the prefix.
 * @param {string} functionName - The name of the function.
 * @param {Array} args - The list of arguments.
 */
function sb_storeUserProperties(functionName, args) {
    const userProperties = PropertiesService.getUserProperties();
    args.forEach((value, index) => {
        userProperties.setProperty(`${functionName}_arg${index + 1}`, value);
    });
    console.log(`Stored properties for ${functionName}:`, JSON.stringify(args));
}

/**
 * Retrieves user properties for a specific function.
 * @param {string} functionName - The name of the function.
 * @returns {Object|string} - The properties for the function or a message if no properties found.
 */
function sb_getUserPropertiesForFunction(functionName) {
    const userProperties = PropertiesService.getUserProperties();
    const allProps = userProperties.getProperties();

    const functionProps = {};
    for (let key in allProps) {
        if (key.startsWith(functionName)) {
            functionProps[key] = allProps[key];
        }
    }

    if (Object.keys(functionProps).length === 0) {
        return "No configuration found.";
    }
    return functionProps;
}

/**
 * Clears the cache for the current script.
 */
function sb_clearCache() {
    const cache = CacheService.getScriptCache();
    cache.removeAll();
}

/**
 * Includes the content of an HTML file in the script.
 * @param {string} filename - The name of the HTML file to include.
 * @returns {string} - The content of the HTML file.
 */
function sb_include(filename) {
    return HtmlService.createHtmlOutputFromFile(filename)
        .getContent()
        .replaceAll("&lt;", "<")
        .replaceAll("&gt;", ">");
}

/**
 * Simulates a long-running process with interleaved short and long messages.
 * @param {string} arg1 - First argument.
 * @param {string} arg2 - Second argument.
 */
function sb_longRunningFunction(arg1, arg2) {
    try {
        throw new Error("Logging stack trace for debugging:");
    } catch (e) {
        console.log(e.stack);
    }

    sidebar.log("Starting the process...");
    Utilities.sleep(400); // Simulate process delay

    sidebar.log("We are part way through the process, making steady progress and moving closer to the next step.");

    Utilities.sleep(200); // Simulate process delay

    for (let i = 0; i < 5; i++) {
        sidebar.log(`Looping iteration ${i + 1}...`);
        Utilities.sleep(100); // Simulate loop delay
    }

    sidebar.log("This is a very long message intended to test the UI's collapsible functionality. This message is extended further to meet the requirement of being 700 characters long. By adding more content, we can ensure that the message testing for the collapsible UI is thorough. The message should be able to expand when clicked and collapse when required, ensuring that the content is properly displayed. Here's more filler content: Lorem ipsum dolor sit amet, consectetur adipiscing elit. Integer nec odio. Praesent libero. Sed cursus ante dapibus diam. Sed nisi. Nulla quis sem at nibh elementum imperdiet. Duis sagittis ipsum. Praesent mauris. Fusce nec tellus sed augue semper porta. Mauris massa. Vestibulum lacinia arcu eget nulla. Class aptent taciti sociosqu ad litora torquent per conubia nostra, per inceptos himenaeos. Curabitur sodales ligula in libero. Sed dignissim lacinia nunc. Curabitur tortor. Pellentesque nibh. Aenean quam. In scelerisque sem at dolor. Maecenas mattis.");

    Utilities.sleep(2000); // Simulate longer delay

    sidebar.log("Finished the process...");
}

// Logger namespace to encapsulate logging functions
var sidebar = (function() {
    // Private function to handle the formatted log messages
    function storeLogMessage(type, format, ...values) {
        const id = extractIdFromFunctionName();
        let message = format.replace(/%s/g, () => values.length ? values.shift() : '%s');

        console[type](message); // Output message to the console using the appropriate log type

        const lock = LockService.getScriptLock();
        try {
            lock.waitLock(10000); // Wait for up to 10 seconds

            const cache = CacheService.getDocumentCache();
            const existingJson = cache.get(id) || "[]";
            const existingMessages = JSON.parse(existingJson);
            existingMessages.push(message);
            cache.put(id, JSON.stringify(existingMessages), 150);
        } catch (e) {
            console.error("Error with logMessage:", e.toString());
        } finally {
            lock.releaseLock();
        }
    }

    // Private function to extract the ID from the function name
    function extractIdFromFunctionName() {
        if (LOG_ID.length > 0) {
            console.log("Log ID: %s", LOG_ID[0].id);
            return LOG_ID[0].id;
        } else {
            throw new Error('No LOG_ID available');
        }
    }

    // Public logging functions
    return {
        log: function(format, ...values) {
            storeLogMessage('log', format, ...values);
        },

        info: function(format, ...values) {
            storeLogMessage('info', format, ...values);
        },

        warn: function(format, ...values) {
            storeLogMessage('warn', format, ...values);
        },

        error: function(format, ...values) {
            storeLogMessage('error', format, ...values);
        }
    };
})();

/**
 * Retrieves process status from the cache.
 * @param {string} id - The process ID.
 * @returns {Object} - The messages associated with the process ID.
 */
function sb_getProcessStatus(id) {
    const lock = LockService.getScriptLock();
    try {
        lock.waitLock(10000); // Wait for up to 10 seconds

        const cache = CacheService.getDocumentCache();
        const messages = cache.get(id) ? JSON.parse(cache.get(id)) : [];
        console.log("Retrieved messages from cache for ID", id, ":", messages);

        // Clear the messages after they've been fetched
        cache.remove(id);

        return { messages: messages };
    } catch (e) {
        console.error("Error with sb_getProcessStatus:", e.toString());
        return { messages: [] };
    } finally {
        lock.releaseLock();
        console.log("Released the lock.");
    }
}

/**
 * Retrieves configuration properties from the script properties.
 * @returns {Array} - An array of configuration objects.
 */
function sb_getConfigProperties() {
    const properties = PropertiesService.getScriptProperties().getProperties();
    const configArray = [];

    for (const key in properties) {
        let type = 'text';
        let label = key;
        let value = properties[key];
        let options = [];

        const underscoreIndex = key.indexOf('_');

        if (underscoreIndex > 0) {
            const possibleType = key.substring(0, underscoreIndex).toLowerCase();
            const possibleLabel = key.substring(underscoreIndex + 1);

            const recognizedTypes = ['date', 'number', 'select', 'textarea'];
            if (recognizedTypes.includes(possibleType)) {
                type = possibleType;
                label = possibleLabel.replace(/([A-Z])/g, ' $1').replace(/_/g, ' ').replace(/^./, function(str) {
                    return str.toUpperCase();
                });
            }
        }

        if (type === 'select') {
            options = ['Option 1', 'Option 2', 'Option 3'];
        }

        configArray.push({ name: key, label: label, type: type, value: value, options: options });
    }

    return configArray;
}

/**
 * Saves user properties for a specific function.
 * @param {string} functionName - The name of the function.
 * @param {Array} inputs - The input values to be saved.
 */
function sb_saveUserPropertiesForFunction(functionName, inputs) {
    const properties = {};

    inputs.forEach((inputValue, index) => {
        const argName = specificButtonConfigs.find(config => config.functionName === functionName).args[index].label;
        properties[`input-${functionName}-${argName}`] = inputValue;
    });

    if (typeof server.saveUserProperties === 'function') {
        server.saveUserProperties(functionName, properties)
            .withSuccessHandler(() => {
                console.log(`Properties for ${functionName} saved successfully.`);
            })
            .withFailureHandler(error => {
                console.error(`Error saving properties for ${functionName}:`, error);
            });
    } else {
        console.warn(`saveUserProperties is not a function on the server. Properties for ${functionName} will not be saved.`);
    }
}

/**
 * Saves configuration properties sent from the client.
 * @param {Array} configData - Array of key-value pairs to be saved.
 * @returns {string} - Confirmation message.
 */
function sb_saveConfigProperties(configData) {
    configData.forEach(function(item) {
        PropertiesService.getScriptProperties().setProperty(item.key, item.value);
    });
    return "Configurations saved successfully.";
}
