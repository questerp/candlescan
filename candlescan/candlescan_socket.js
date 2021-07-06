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
	socket.emit('welcome',socket.id);
	//socket.on('candlesocket',function(data){
	//	console.log('getting msg from web',data);
	//});
})



var conf = get_conf();
var subscriber = get_redis_subscriber();
subscriber.on('message',function(channel,message){
	
	if(channel=='candlescan_single'){
		message = JSON.parse(message);
		if(message.socket_id) {
			sockets = await io.in(message.socket_id).fetchSockets();
			if(sockets){
				socket = sockets[0];
				socket.emit(message.scanner_id,message.data);
			}
			
		}
	}
	if(channel=='candlescan_all'){
		message = JSON.parse(message);
		if(message.scanner_id) {
			io.sockets.emit(message.scanner_id,message.data);
		}
	}

});
subscriber.subscribe('candlesocket');
