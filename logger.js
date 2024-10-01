var LOG_ID = [];

const ALLOWED_PROPERTIES = [
    {
        key: 'STRIPE_API_KEY',
        label: 'Stripe API Key',
        type: 'password', // 'text', 'password', 'textarea', 'select', etc.
        required: true,
        tooltip: 'Your secret Stripe API key.',
        scope: 'script' // Allowed scopes: 'script'
    },
    {
        key: 'RECEIPTS_FOLDER_URL',
        label: 'Receipts Folder URL',
        type: 'url',
        required: true,
        tooltip: 'The Google Drive folder URL where receipts will be saved.',
        scope: 'document' // Allowed scopes: 'user', 'document'
    },
    {
        key: 'STRIPE_PAYOUT_DESCRIPTION_PREFIX',
        label: 'Stripe Payout Description Prefix',
        type: 'text',
        required: true,
        tooltip: 'Prefix for payout descriptions in the sheet.',
        scope: 'user' // Allowed scopes: 'user'
    },
    {
        key: 'SUMMARY_EMAIL',
        label: 'Summary Email',
        type: 'email',
        required: true,
        tooltip: 'Email address to receive summary reports.',
        scope: 'document' // Allowed scopes: 'user', 'document'
    }
    // Add more properties as needed
];


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
 * Stores input arguments in document properties with the function name as the prefix.
 * @param {string} functionName - The name of the function.
 * @param {Array} args - The list of arguments.
 */
function sb_storeUserProperties(functionName, args) {
    const docProperties = PropertiesService.getDocumentProperties();
    args.forEach((value, index) => {
        docProperties.setProperty(`${functionName}_arg${index + 1}`, value);
    });
    console.info(`Stored properties for ${functionName}:`, JSON.stringify(args));
}

/**
 * Retrieves user properties for a specific function.
 * @param {string} functionName - The name of the function.
 * @returns {Object|string} - The properties for the function or a message if no properties found.
 */
function sb_getUserPropertiesForFunction(functionName) {
    const docProperties = PropertiesService.getDocumentProperties();
    const allProps = docProperties.getProperties();

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
 * Saves user properties for a specific function.
 * @param {string} functionName - The name of the function.
 * @param {Object} properties - An object containing key-value pairs of properties.
 * @returns {string} - Confirmation message.
 */
function sb_saveUserPropertiesForFunction(functionName, properties) {
    const docProperties = PropertiesService.getDocumentProperties();
    for (let key in properties) {
        docProperties.setProperty(`${functionName}_${key}`, properties[key]);
    }
    console.info(`Saved user properties for ${functionName}:`, JSON.stringify(properties));
    return "User properties saved successfully.";
}


/**
 * Saves configuration properties sent from the client.
 * Ensures only predefined properties are saved and respects their allowed scopes.
 * @param {Array} configData - Array of key-value-scope objects to be saved. Each item should have 'key', 'value', and 'scope'.
 * @returns {string} - Confirmation message.
 */
function sb_saveConfigProperties(configData) {
    configData.forEach(({ key, value, scope }) => {
        const propertyDef = ALLOWED_PROPERTIES.find(prop => prop.key === key);

        if (!propertyDef) {
            console.warn(`Attempt to set unauthorized property: ${key}`);
            return; // Skip unauthorized properties
        }

        try {
            const confirmation = ConfigManager.setProperty(key, value, scope);
            console.info(confirmation);
        } catch (error) {
            console.error(`Error setting property "${key}": ${error.message}`);
        }
    });

    return "Configurations saved successfully.";
}


/**
 * Retrieves the value of a property by its name.
 * @param {string} propertyName - The name of the property to retrieve.
 * @returns {string|null} - The value of the property or null if not found.
 */
function sb_getProperty(propertyName) {
    return ConfigManager.getProperty(propertyName);
}

/**
 * Retrieves the current configuration settings for all allowed properties.
 * Each property includes its full definition and current value.
 * @returns {Array<Object>} - Array of property objects containing metadata and current values.
 */
function sb_getConfigProperties() {
    return ALLOWED_PROPERTIES.map(prop => ({
        ...prop,
        value: sb_getProperty(prop.key) || ''
    }));
}
// ===========================
// Private Configuration Manager
// ===========================
const ConfigManager = (() => {
    /**
     * Retrieves the value of a property by checking allowed scopes in order.
     * @param {string} propertyName - The name of the property to retrieve.
     * @returns {string|null} - The value of the property or null if not found.
     */
    const getProperty = (propertyName) => {
        // Find the property definition from ALLOWED_PROPERTIES
        const propertyDef = ALLOWED_PROPERTIES.find(prop => prop.key === propertyName);
        if (!propertyDef) {
            console.warn(`Property "${propertyName}" is not allowed.`);
            return null;
        }

        // Determine the scopes to check based on allowed scope
        const allowedScopes = (() => {
            switch (propertyDef.scope.toLowerCase()) {
                case 'script':
                    return ['user', 'document', 'script'];
                case 'document':
                    return ['user', 'document'];
                case 'user':
                    return ['user'];
                default:
                    console.warn(`Unknown scope "${propertyDef.scope}" for property "${propertyName}".`);
                    return [];
            }
        })();

        for (const scope of allowedScopes) {
            const value = (() => {
                switch (scope) {
                    case 'user':
                        return PropertiesService.getUserProperties().getProperty(propertyName);
                    case 'document':
                        return PropertiesService.getDocumentProperties().getProperty(propertyName);
                    case 'script':
                        return PropertiesService.getScriptProperties().getProperty(propertyName);
                    default:
                        return null;
                }
            })();

            if (value !== null) {
                return value;
            }
        }

        // Property not found in any allowed scope
        return null;
    };

    /**
     * Sets the value of a property in the specified scope after validating allowed scopes.
     * @param {string} propertyName - The name of the property to set.
     * @param {string} value - The value to set for the property.
     * @param {string} [scope='user'] - The scope to set the property in ('user', 'document', 'script').
     * @returns {string} - Confirmation message.
     * @throws Will throw an error if the property is not allowed or scope is invalid.
     */
    const setProperty = (propertyName, value, scope = 'user') => {
        // Find the property definition from ALLOWED_PROPERTIES
        const propertyDef = ALLOWED_PROPERTIES.find(prop => prop.key === propertyName);
        if (!propertyDef) {
            throw new Error(`Property "${propertyName}" is not allowed.`);
        }

        // Normalize scope input
        const normalizedScope = scope.toLowerCase();

        // Determine allowed scopes for setting based on property's allowed scope
        const permittedScopes = (() => {
            switch (propertyDef.scope.toLowerCase()) {
                case 'script':
                    return ['user', 'document', 'script'];
                case 'document':
                    return ['user', 'document'];
                case 'user':
                    return ['user'];
                default:
                    return [];
            }
        })();

        if (!permittedScopes.includes(normalizedScope)) {
            throw new Error(`Scope "${scope}" is not permitted for property "${propertyName}". Allowed scopes: ${permittedScopes.join(', ')}.`);
        }

        // Set the property in the specified scope
        switch (normalizedScope) {
            case 'user':
                PropertiesService.getUserProperties().setProperty(propertyName, value);
                break;
            case 'document':
                PropertiesService.getDocumentProperties().setProperty(propertyName, value);
                break;
            case 'script':
                PropertiesService.getScriptProperties().setProperty(propertyName, value);
                break;
            default:
                throw new Error(`Invalid scope "${scope}". Valid scopes are 'user', 'document', 'script'.`);
        }

        return `Property "${propertyName}" set successfully in "${normalizedScope}" scope.`;
    };

    // Expose only internal functions if needed, or keep them completely private
    return {
        getProperty,
        setProperty
    };
})();


