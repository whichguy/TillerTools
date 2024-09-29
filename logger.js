var LOG_ID = 1000 ;

function invokeWithId(functionName, id, ...args) 
{    
  LOG_ID = id ;
  console.log("Starting Function [%s] %s: %s", id, JSON.stringify(functionName), JSON.stringify(args) ) ;
  
  if ( args && args.length > 0 )
    return this[functionName](args); 
  else 
    return this[functionName]() ;
}

function clearCache() {
  const cache = CacheService.getUserCache();
  cache.removeAll();
}


function logMessage(...args) 
{
    let   arg = 1 ;
    const replace = args.slice(1) ;
    const message = args.length > 1 ? 
      args[0].replaceAll("%s", (match) => replace.shift() ) :
      args[0] ;

    const id = LOG_ID ;

    console.log("logMessage: [%s]: %s", id, message );

    const cache = CacheService.getUserCache();

    const lock  = LockService.getUserLock();


    try {
        lock.waitLock(10 * 1000 ); // Wait for up to 10 seconds
        const existingJson = cache.get(id) || "[]" ;
        const existingMessages = JSON.parse(existingJson) ;
        existingMessages.push(message) ;
        cache.put(id, JSON.stringify(existingMessages), 150);
    } catch (e) {
        console.error("Error with logMessage:", e.toString());
    } finally {
        lock.releaseLock();
    }

    // console.log("Finished logMessage function.");
}

function getProcessStatus(id) {
    console.log("Starting getProcessStatus function...");
    const cache = CacheService.getUserCache();

    console.log("Attempting to acquire lock for %s", id );
    const lock = LockService.getUserLock() ;
    
    try {
        lock.waitLock(10 * 1000); // Wait for up to 10 seconds

        const messages = cache.get(id) ? JSON.parse(cache.get(id)) : [];
        // console.log("Retrieved messages from cache for ID", id, ":", messages);

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
}


