module.exports = function(context, sbMsg) {
    context.log('LoggingServiceBusQueueTriggerJS1 processed message:', sbMsg);
    context.bindings.outputBlob = sbMsg;
    context.done();
};