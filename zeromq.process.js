var zmq = require('zmq');

//Create a new object
// type can be either 'worker' or 'master'
var _export = function(type){
    
    var base = this;
    
    if(type !== 'worker' && type !== 'master') throw 'zmqp - Incorrect type "' + type + '"';
    
    //Set the type of ZMQp object this is (master or worker)
    base.type = type;
    
    //MASTER use only
    // Requests waiting to be processed
    // could be from .process or .wait
    // queue = {
    //      "id":[{request},{request},..],
    //}
    base.queue = {};
    
    //Heartbeat timeouts
    base.heartbeatTimeout = {};
    
    //ZMQp options
    base.options = {
        
         //Spit any error messages to the console
        'debug':false,

        //Trace message flow
        'trace':false,
        
        //Heartbeat timeout
        // default no heartbeat
        'heartbeat':0,
        
        //Request timeout
        'timeout':1000
    };
};


//------------------------------ Shared Methods -----------------------------------

//Connect to port and list to available nodes
_export.prototype.connect = function(id,port,available,ready) {
    var base = this;

    //Available masters
    base.available = available;
    
    //Our current id
    base.id = id;
    
    //Create a new subscriber
    base.sub = zmq.socket('sub');

    //Connect to the list of available publishers
    for(var i in base.available) {
        
        //Connect to the publisher
        base.sub.connect('tcp://' + base.available[i]);

        //Set the heartbeat timeout
        // if we need one
        if(base.options.heartbeat) {
            
            //Set the heartbeat timeout
            base._setHeartbeatInterval(i,base.available[i]);
        }
    }
    
    //Subscribe to our channel
    //  and all channel
    base.sub.subscribe(base.id);
    base.sub.subscribe('all');
    
    if(base.options.debug) console.log('TRACE zmqp.connect - Subscribing to channels ["all","' + base.id + '"]');
    
    //Setup message receiving for the subscriber
    base.sub.on('message', function(data){
        base._receive(data);
    });
    
    //Create a new publisher
    base.pub = zmq.socket('pub');
     
    //Bind to a particular port
    base.pub.bind('tcp://0.0.0.0:'+port, function(error) {
     
        if(ready) ready(error);
        
        //Setup the heartbeat if we don't have an error
        if(!error) {
        
            //Setup the heartbeat if needed
            if(base.options.heartbeat) {
                
                //Trace message
                if(base.options.debug) console.log('DEBUG zmqp.connect - Sending heartbeat every ' + base.options.heartbeat + 'ms');
                
                //Set the worker timeout
                setInterval(function() {
                    
                    //Send heartbeat message
                    base.pub.send( message.get('all',base.id,'heart','-',1) );    
                    
                },base.options.heartbeat*0.9);
            }
        }
        
    });    
    
    //Set the timeout
    if(base.options.debug && base.options.timeout) console.log('DEBUG zmqp.connect - Setting worker ready timeout to ' + base.options.timeout + 'ms');
};

_export.prototype.debug = function(_debug) {
    
    var base = this;
    
    base.options.debug = _debug;
};

_export.prototype.trace = function(_trace) {
    
    var base = this;
    
    base.options.trace = _trace;
};

_export.prototype.heartbeat = function(_interval){
    
    var base = this;
    
    base.options.heartbeat = _interval;
};

_export.prototype.timeout = function(_timeout){
    
    var base = this;
    
    base.options.timeout = _timeout;
};

//------------------------------ Public Methods -----------------------------------
//For both worker and master
//  worker obj == process callback
//  master obj == obj to process && callback == callback when processing done
//   or obj,callback
_export.prototype.process = function(obj,id,callback) {
    
    var base = this;
    
    //Use of the function with only two inputs
    if(typeof id === 'function') {
        callback = id;
        id = undefined;
    }
    
    switch(base.type){
        
        case 'worker':
            //Save as process callback
            base.process = obj;
        break;
        
        case 'master':
            //Generate a unique id for this object to be processed 
            // if we don't already have one
            if(!id) id = guid.get();

            //Create a place in the queue for this id (if required)
            if(!base.queue[id]) base.queue[id] = {
                "callback":[],
                "obj":obj,
                "worker":undefined,
                "timeout":undefined
            };
            
            //Create a new callback for this request
            base.queue[id].callback.push(callback);
            
            //Are we the first to request to process?
            // if so then make sure we ask the workers who's avail
            // otherwise we'll assume that someone else has already done this
            if(base.queue[id].callback.length === 1) {
                
                //We only do this once per id
                if(base.options.trace) console.log('TRACE zmqp.process - Signaling available workers for ' + id);
                
                //Set timeout for callback so we don't wait forever for a worker to be available
                if(base.options.timeout) {
                    
                    //Save the timeout so we can clear it later if worker responded
                    base.queue[id].timeout = setTimeout(function() {
                        
                        //Signal that we're done, with a timeout error
                        base._done({"id":id,"status":'0',"message":"ERROR zmqp.process - Timeout reached, no availalbe workers within " + base.options.timeout + "ms"});
                        
                        //Log this error if required
                        if(base.options.debug) console.error("ERROR zmqp.process - Timeout reached, no availalbe workers within " + base.options.timeout + "ms");

                    },base.options.timeout);
                }
                
                //Send to all workers to see who's available for processing
                base.pub.send( message.get('all',base.id,'avail',id,1) );
            }
        break;
    }
};

//Master only
// Wait for this object to be processed
// ONLY use this if we have multiple masters (otherwise just use process and we'll take care of who should wait)
_export.prototype.wait = function(id,callback) {
    
    var base = this;
    
    //Create a place in the queue for this id (if required)
    if(!base.queue[id]) base.queue[id] = {
        "callback":[],
        "obj":undefined,
        "worker":undefined,
        "timeout":undefined
    };
    
    //Create a new callback for this request
    base.queue[id].callback.push(callback);
    
    if(base.options.trace) console.log('TRACE zmqp.wait - Waiting for completion of ' + id);
};

//------------------------------ Shared Events -----------------------------------

//Subscribe on receive event
_export.prototype._receive = function(data) {
 
    var base = this;  
    
    //Parse the message
    var msg = message.parse(data.toString());
    
    //if(base.options.trace) console.log('TRACE zmqp._receive - Received message of type ' + msg.type);
    
    //Make sure the message is valid
    if(msg) {
        switch(msg.type) {
            
            //------------------ Shared Events ---------------------
            
            case 'heart':
                base._heartbeat(msg);
            break;
            
            //------------------ Worker Events ---------------------
            case 'avail':
                base._avail(msg);
            break;
            
            case 'process':
                base._process(msg);
            break;
            
            //------------------ Master Events ---------------------
            
            case 'ready':
                base._ready(msg);
            break;
            
            case 'done':
                base._done(msg);
            break;
            
            
            //------------------ Default Error ---------------------
            default:
                if(base.options.debug) console.error("ERROR zmqp._receive - Unrecognized event type" + msg.type);
            break;
        }
    } else {
        
        if(base.options.debug) console.error('ERROR zmqp._receive - Dropping incorrectly formed message ' + data.toString());
    }
};

//------------------------------ Heartbeat Events -----------------------------------

//Set the heartbeat timeout
_export.prototype._setHeartbeatInterval = function(name,tcp) {
    
    var base = this;
    
    //Set the new heartbeat timeout
    base.heartbeatTimeout[name] = setInterval(function() {
        
        //Try to reconnect if we reach the timeout
        base._reconnect(tcp);
        
    },base.options.heartbeat);
};

//Reconnect to this server
_export.prototype._reconnect = function(tcp) {
    
    var base = this;
    
    if(base.options.trace) console.log('TRACE zmqp._reconnect - Trying reconnect with ' + tcp);
    
    //Disconnect 
    // insures that we don't connect twice
    base.sub.disconnect('tcp://' + tcp);
    
    //Try to reconnect
    base.sub.connect('tcp://' + tcp);
};

//Heart beat received
_export.prototype._heartbeat = function(msg) {
    var base = this;  
    
    //Contact the requester of this heart beat back with an ACK
    //Trace message
    if(base.options.trace)
        if(base.options.trace.heartbeat) console.log('TRACE zmqp._heartbeat - Heartbeat received from ' + msg.from);
    
    //Get the server info for this heartbeat
    var tcp = base.available[msg.from];
    
    //Check to see if we have a server tcp address
    if(tcp && base.heartbeatTimeout[msg.from]) {
        //Clear the existing timeout
        clearInterval( base.heartbeatTimeout[msg.from] );
        
        //Set a new heartbeat interval
        base._setHeartbeatInterval(msg.from,tcp);
    }
};

//------------------------------ Master Events -----------------------------------

//Worker signaled that they are ready for processing
_export.prototype._ready = function(msg) {
    var base = this;  

    //Get the waiting request from the queue
    var current = base.queue[msg.id];
    
    if(!current) return;
        
    //Make sure that this object isn't already being processed by another worker
    // Insures that when multiple workers respond the first one gets to process it
    if(current.worker === undefined) {
    
        //Clear the timeout
        clearTimeout(base.queue[msg.id].timeout);
    
        //Set this worker to this processing object
        base.queue[msg.id].worker = msg.from;
        
        //Trace message
        if(base.options.trace) console.log('TRACE zmqp._ready - Master received ready message from ' + msg.from + ' for ' + msg.id + ' => sent process request to this worker');
        
        //Send to the first worker for processing
        base.pub.send( message.get(msg.from,base.id,'process',msg.id,1,current.obj) );
    }
};

//Worker signaled that they are done
// make sure to perform all the waiting callbacks
_export.prototype._done = function(msg) {
 
    var base = this;  
    
    //Get the object that we are looking to process from the current stack
    var current = base.queue[msg.id];
    
    if(!current) return;

    //Message should contain two parts
    // status message
    if(!msg.status) {
        var err = "ERROR zmqp._done - Done message missing status";
        if(base.options.debug) console.error(err);
        base._callback(current.callback,err);
        return;
    }
    
    if(base.options.trace) console.log('TRACE zmqp._done - Master received done message from ' + msg.from + ' for ' + msg.id + ' with status ' + msg.status + ' (' + msg.message + ')');
    
    //Perform the callback to signal that we are finished processing this object
    // message should contain the status as well as an optional message
    if(msg.status === '0') base._callback(current.callback,msg.message);
    else base._callback(current.callback,undefined,msg.message);
    
    //Finally remove this one from the queue
    // as we are done with it
    delete base.queue[msg.id];
};

_export.prototype._callback = function(callbacks,err,success){
    
    //Make all the callbacks
    for(var i=0; i < callbacks.length; i++)
        callbacks[i](err,success);
};


//------------------------------ Worker Events -----------------------------------

//Master asked for availablity
_export.prototype._avail = function(msg) {
 
    var base = this;  

    //Signal that we are available
    //TODO add in ability to check for availablity
    // perhaps callback??
    
    if(base.options.trace) console.log('TRACE zmqp._avial - Worker received avail request from ' + msg.from + ' for ' + msg.id + ' => sent ready request to master');

    //Signal master that we are ready for work
    base.pub.send( message.get(msg.from,base.id,'ready',msg.id,1) );
};

//Master signaled that they want us to process this request!
_export.prototype._process = function(msg) {
 
    var base = this;  
    
    //Check the message to insure we have the details on what to process
    if(!msg.message) {
        var err = "ERROR zmqp._process - Available message missing master id";
        if(base.options.debug) console.error(err);
        return;
    }
    
    if(base.options.trace) console.log('TRACE zmqp._process - Worker received process request for ' + msg.id + ' with data ' + msg.message);
    
    //Process this object (obj in message)
    base.process(msg.message,function(err,done){
    
        var _message;
        var status = err ? 0 : 1;
    
        //Check if we had an error
        if(err) _message = err;
        else if(done) _message = done;
        
        if(base.options.trace) console.log('TRACE zmqp._process - Worker finished processing request for ' + msg.id + ' with status ' + status + ' => broadcasting done message');
        
        //Send the status to all masters
        base.pub.send( message.get('all',base.id,'done',msg.id,status,_message) );
    });
};


//------------------------------ Shared -----------------------------------
var message = {
    'get':function(to,from,type,id,status,msg){
    
        var packet = [to,from,type,id,status];
        
        if(msg) packet.push(msg);
        
        return(packet.join(' '));
    },
    
    'parse':function(data) {
    
        data = data.split(' ');
        
        if(data.length < 5) {
            if(base.options.debug) console.error("ERROR zmqp.message - Not enough parameters in message");
            return;
        }
        
        var msg;
        
        //Check to see if we have a message
        if(data.length > 5) msg = data.slice(5, data.length).join(' ');
        
        return({
           'to':data[0],
           'from':data[1],
           'type':data[2],
           'id':data[3],
           'status':data[4],
           'message':msg
        });
    }
};

var guid = {
	'get': function() {
        return (S4()+S4()+"-"+S4()+"-"+S4()+"-"+S4()+"-"+S4()+S4()+S4());
	}
};

function S4() {
        return (((1+Math.random())*0x10000)|0).toString(16).substring(1);
}

//------------------------------ Exports -----------------------------------
module.exports = _export;