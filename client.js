// Run via `node client.js`.

function DBEvent(record, eventType)
{
    this.triggers = [] // array of observer/trigger functions (Added, Removed, Updated)
    this.record = record
    this.eventType = eventType
    this.insertedAt = Math.floor(Math.random()*1000000) // simulate timestamp without importing Datejs/moment etc. 
    this.retries = 0 
}

DBEvent.prototype = {
    // add trigger to DBEvent
    addTrigger: function(fn) 
    {
        this.triggers.push(fn)
    },
    process: function() {
        this.triggers.forEach(fn => {
            // Try...Catch Block to handle Trigger failures. 
            try {
                fn.call()
            } catch (error) { 
                console.log("error:", error)
            }

        })
    },
    clear: function() {
        this.triggers = []
    },

}


// Trigger functions (simulate webhooks)
function Added() {
    // TODO: Move the Try Catch down to Webhook trigger level and add to RetryQueue. 
    console.log(`added record:${record}`)
}

function Updated() {
    // TODO: Move the Try Catch down to Webhook trigger level and add to RetryQueue. 
    console.log(`updated record:${record}`)
}

function Removed() {
    // TODO: Move the Try Catch down to Webhook trigger level and add to RetryQueue. 
    console.log(`removed record:${record}`)
}

function FailToProcess() {
    throw 'failed to process message'
}

let eventQueue = [] // FIFO Message Queue (Push/Pop the 0th elem) - Simulate SQS.  
let retryQueue = [] // Queue for Messages that fail to process.

const remove = "remove"
const add = "add"
const update = "update"
const maxRetries = 3

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

// Adding an event that will fail to process
eventQueue.push(new DBEvent("f@k3_cu$t0m3r", "errorOut"))

console.log("initial eventQueue", eventQueue)

// While message queue is not empty, keep popping the 0th elem (shift).
while (eventQueue.length != 0) {
    currEvent = eventQueue.shift()
    console.log("just recieved event:", currEvent)

    if (currEvent.retries < maxRetries) {
        switch(currEvent.eventType){
            case add:
                record = currEvent.record
                currEvent.addTrigger(Added)
                break;
            case update:
                record = currEvent.record
                currEvent.addTrigger(Updated)
                break;
            case remove:
                record = currEvent.record
                currEvent.addTrigger(Removed)
                break;
            default:
                console.log(`eventType: ${currEvent.eventType} not associated with a trigger function`)
                currEvent.retries += 1
                retryQueue.push(currEvent)
        }
        currEvent.process()

        // Once event successfully processes, remove the notification webhook(s). 
        currEvent.clear() 
        // TODO: update the DB lastProcessed table with timestamp of the 
        // event processed after currEvent.process() and clear(). 

        // In the event of client failure, client will pull
        // all _sync_events (DB events) from the _sync_events table that 
        // have `inserted_at` > timestamp of the very last lastProcessed
        // record. In effect, we are using timestamp as a pagination cursor. 
        // Wasn't sure how to simulate that exactly, so leaving this comment instead. 
    }

    // When there are events to retry, push them onto the main EventQueue. 
    while (retryQueue.length != 0) {
        retryEvent = retryQueue.shift()
        eventQueue.push(retryEvent)
    } 

    // Simulate client polling message queue for new messages.
    const hasNewEvent = Math.random() < 0.5 // 50% of time, there is a new event in the message queue. 
    // console.log("hasNewEvent", hasNewEvent)
    if (hasNewEvent) {
        const hasError = Math.random() < 0.1 // Introduce random error
        if (hasError) {
            eventQueue.push(new DBEvent("SBF", FailToProcess))
        } else {
            eventQueue.push(new DBEvent("CZ", add))
        }
    }

}