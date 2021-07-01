var fs = require('fs');
var path = require('path');
//var redis = require('redis');
var { get_conf, get_redis_subscriber } = require('../../frappe/node_utils');
var app = require('../../frappe/node_modules/express')();
var server = require('http').Server(app);
var io = require('../../frappe/node_modules/socket.io')(server,{
	cors:{
		origin: "*",
	}
});


server.listen(9001);

io.on('connection',function(socket){
	//console.log("connected");
	socket.emit('candlesocket',"Welcome");
	socket.on('candlesocket',function(data){
		
		console.log('getting msg from web',data);

	});
})



var conf = get_conf();

//var subscriber = redis.createClient(conf.redis_socketio || conf.redis_async_broker_port);

var subscriber = get_redis_subscriber();

subscriber.on('message',function(channel,message){

	if(channel=='candlesocket'){
		message = JSON.parse(message);
		console.log('channel',channel);
		//console.log('socket MESSAGE',message);
		if(message) {
			console.log("Broadcasting",message);
			io.sockets.emit('candlesocket',message);
		}
	}

});
//subscriber.subscribe('events');
subscriber.subscribe('candlesocket');
