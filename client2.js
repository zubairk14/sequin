// Run via `node client2.js`.

let eventQueue = [] // FIFO Message Queue (Push/Pop the 0th elem) - Simulate SQS.  
let retryQueue = [] // Queue for Messages that fail to process.

const remove = "remove"
const add = "add"
const update = "update"
const maxRetries = 3

function DBEvent(record, eventType)
{
    this.record = record
    this.eventType = eventType
    this.insertedAt = Math.floor(Math.random()*1000000) // simulate timestamp without importing Datejs/moment etc. 
    this.retries = 0 
    this.error = false
}

function DBEventListener() {
    this.addHandlers = []
    this.removeHandlers = []
    this.updateHandlers = []
}

DBEventListener.prototype = {
    register: function(eventType, fn) {
        if (eventType === add) {
            this.addHandlers.push(fn)
        }
        if (eventType === remove) {
            this.removeHandlers.push(fn)
        }
        if (eventType === update) {
            this.updateHandlers.push(fn)
        }
    },
    handleEvent: function(event) {
        if (event.eventType === add) {
            this.addHandlers.forEach(fn => {
                try {
                    fn.call(this, event)
                } catch (error) {
                    event.retries += 1
                    retryQueue.push(event)
                    console.log(error)
                }
            })
        }
        if (event.eventType === update) {
            this.updateHandlers.forEach(fn => {
                try {
                    fn.call(this, event)
                } catch (error) {
                    event.retries += 1
                    retryQueue.push(event)
                    console.log(error)
                }
            })
        }
        if (event.eventType === remove) {
            this.removeHandlers.forEach(fn => {
                try {
                    fn.call(this, event)
                } catch (error) {
                    event.retries += 1
                    retryQueue.push(event)
                    console.log(error)
                }
            })
        }
    }
}

// Handlers (basically webhooks to Slack, Email, SMS, etc)

function Added(event) {
    // TODO: Move the Try Catch down to Webhook trigger level and add to RetryQueue. 
    console.log("added record", event.record)
}

function Removed(event) {
    // TODO: Move the Try Catch down to Webhook trigger level and add to RetryQueue. 
    if (event.error) {
        throw "ERROR: SBF broke this"
    }
    console.log("removed record", event.record)
    
}

function Updated(event) {
    // TODO: Move the Try Catch down to Webhook trigger level and add to RetryQueue. 
    console.log("updated record", event.record)
}


// Instantiate DB event listener and register all the handlers. 

const dbEventListener = new DBEventListener
console.log("listen", dbEventListener)

dbEventListener.register('add', Added)
dbEventListener.register('remove', Removed)
dbEventListener.register('update', Updated)

// Seed message queue with 10 events.  
for (let i = 0; i < 10; i++) {
    let record = "customer " + i.toString()
    let eventType 
    // Programmatically assign event types. 
    if (i % 5 == 0) {
        eventType = remove
    } else if (i % 2 == 0) {
        eventType = update
    } else {
        eventType = add
    }
    currEvent = new DBEvent(record, eventType)
    eventQueue.push(currEvent)
}

console.log("initial eventQueue", eventQueue)



// While message queue is not empty, keep popping the 0th elem (shift).
while (eventQueue.length != 0) {
    currEvent = eventQueue.shift()
    console.log("just recieved event:", currEvent)

    if (currEvent.retries < maxRetries) {
        dbEventListener.handleEvent(currEvent)
    }

    // When there are events to retry, push them onto the main EventQueue. 
    while (retryQueue.length != 0) {
        retryEvent = retryQueue.shift()
        eventQueue.push(retryEvent)
    } 

    // Simulate client polling message queue for new messages.
    const hasNewEvent = Math.random() < 0.5 // 50% of time, there is a new event in the message queue. 
    console.log("hasNewEvent", hasNewEvent)
    if (hasNewEvent) {
        const hasError = Math.random() < 0.1 // Introduce random error
        if (hasError) {
            errorEvent = new DBEvent("SBF", remove)
            errorEvent.error = true
            eventQueue.push(errorEvent)
        } else {
            eventQueue.push(new DBEvent("CZ", add))
        }
    }

}
