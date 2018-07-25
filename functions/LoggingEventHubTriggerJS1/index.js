module.exports = function (context, eventHubMessages) {
    context.log(`LoggingEventHubTriggerJS1 message array: ${eventHubMessages}`);
    
    eventHubMessages.forEach(message => {
        context.log(`Processed message: ${message}`);
        context.bindings.outputBlob = message;
    });

    context.done();
};