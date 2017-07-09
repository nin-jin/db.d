import vibe.core.core;
import vibe.data.json;
import vibe.http.server;
import vibe.http.router;
import vibe.http.websockets;
import core.time;
import std.stdio;
import std.outbuffer;
import std.range;
import std.conv;
import std.traits;
import std.algorithm;
import std.typecons;
import std.variant;
import std.meta;
import std.digest.murmurhash;

class Store
{
	static string path= "./store/";

	static Stream[] streams;
	static Var* root;

	static auto stream( const ushort id )
	{
		while( id >= streams.length ) {
			streams ~= new Stream( path ~ streams.length.to!string ~ ".dbd" );
		}
		return streams[ id ];
	}

	static auto put( Record )( Record record )
	{
		return immutable Var( 1 , stream(1).put( record ) );
	}

	static auto make( Record , Args... )( Args args )
	{
		return Store.put( immutable( Record )( args ) );
	}

	static auto var( string data )
	{
		if( data.length > 6 ) return Store.put( data );
		else return immutable Var( data );
	}

	static auto var( ushort stream , uint offset )
	{
		return immutable Var( stream , offset );
	}

	static commit( Var root )
	{
		Store.root = &root;
		yield;
	}

	static sync()
	{
		if( !Store.root ) return;
		
		if( streams.length > 0 ) {
			foreach( stream ; streams[ 1 .. $ ] ) stream.sync;
		}
		
		stream(0).put( *Store.root );
		stream(0).sync;
		
		Store.root = null;
	}

	static this()
	{
		runTask({
			while( true ) {
				16.msecs.sleep;
				Store.sync;
			}
		});
	}

}

class Stream
{
	File file;

	this( string path )
	{
		this.file = File( path , "a+b" );

		if( this.file.tell == 0 ) this.file.lockingBinaryWriter.put( "dbd\n" );
	}

	uint put( Data )( Data data ) if( !isDynamicArray!Data )
	{
		auto offset = this.file.tell.to!uint;
		
		this.file.lockingBinaryWriter.put( data );
		
		return offset;
	}

	uint put( Data )( Data data ) if( isDynamicArray!Data )
	{
		auto offset = this.file.tell.to!uint;
		
		auto writer = this.file.lockingBinaryWriter;

		writer.put( data.length.to!uint );
		writer.put( data );
		
		auto pad = 4 - data.length % 4;
		if( pad != 4 ) writer.put( "    "[ 0 .. pad ] );

		return offset;
	}

	Stream seek( uint offset )
	{
		this.file.seek( offset );
		return this;
	}

	auto read( Type )() if( !isDynamicArray!Type )
	{
		Unqual!Type[1] buffer;
		this.file.rawRead( buffer );
		return cast( immutable ) buffer[0];
	}

	immutable( Type ) read( Type )() if( isDynamicArray!Type )
	{
		auto buffer = new Unqual!( ElementEncodingType!Type )[ this.read!uint ];
		this.file.rawRead( buffer );
		return cast( immutable ) buffer;
	}

	void sync()
	{
		this.file.flush;
		this.file.sync;
	}

}

immutable struct Var
{
align(2):

	enum {
		String0 = 0 ,
		String1 = 1 ,
		String2 = 2 ,
		String3 = 3 ,
		String4 = 4 ,
		String5 = 5 ,
		String6 = 6 ,
		Link = 7 ,
		Leaf = 8 ,
		Branch = 9 ,
	}

	ushort type;

	union
	{
		struct
		{
			ushort stream;
			uint offset;
		}

		char[6] data;
	}

	auto input()
	{
		return Store.stream( this.stream ).seek( this.offset );
	}

	alias input this;

	this( string data )
	{
		if( data.length > 6 ) throw new Exception( "Var data length must less or equal 6" );

		this.type = data.length.to!ushort;
		this.data[ 0 .. data.length ] = data;
	}

	this( ushort stream , uint offset )
	{
		this.type = 7;
		this.stream = stream;
		this.offset = offset;
	}

	string toString()
	{
		switch( this.type ) {
			case String0 : 
			case String1 : 
			case String2 : 
			case String3 : 
			case String4 : 
			case String5 : 
			case String6 :
					return "[" ~ this.data[ 0 .. this.type ] ~ "]";
			
			case Link : return this.stream.to!string ~ "/" ~ this.offset.to!string;
			
			default : assert(0);
		}
	}

}

uint hash( string data )
{
	auto res = digest!( MurmurHash3!32 )( data );
	return * cast(uint*) cast(void*) &res;
}

/+
immutable struct Branch
{
	Var[2] branches;

	Var get( uint hash )
	{
		auto var = this.branches[ hash & 0b1 ];
		
		switch( var.type ) {
			case Var.Branch :
				auto dict = var.read!Branch;
				return dict.get( hash >> 1 );
			case Var.Leaf :
				auto leaf = var.read!Leaf;
				return leaf.get( hash >> 1 );
			default :
				assert( 0 );
		}
	}

	Var put( uint hash , Var value )
	{
		auto index = hash & 0b1;
		auto var = this.branches[ index ];

		switch( var.type ) {
			case Var.Branch :
				auto dict = var.read!Branch;
				auto branch = dict.put( hash >> 1 , value );
				return Store.make!Branch([ index == 0 ? branch : this.branches[0] , index == 1 ? branch : this.branches[1] ]);
			case Var.Leaf :
				auto leaf = var.read!Leaf;
				auto branch = leaf.put( hash >> 1 , value );
				return Store.make!Branch([ index == 0 ? branch : this.branches[0] , index == 1 ? branch : this.branches[1] ]);
			default :
				assert( 0 );
		}
	}
}

immutable struct Leaf( Target )
{
	uint hash;
	Var target;

	Var get( uint hash )
	{
		if( hash == this.hash ) return this.target;
		return Store.var( 0,0 );
	}

	Var put( uint hash , Var value )
	{
		if( hash == this.hash ) return Store.make!Leaf( hash , value );

		return Store.make!Branch([]);
	}
}
+/

immutable struct Comment
{
align(4):

	Var parent;
	Var message;

	string toString()
	{
		return "Comment{ parent:" ~ this.parent.to!string ~ " message:" ~ this.message.to!string ~ " }";
	}
}

static this()
{
	auto settings = new HTTPServerSettings;
	settings.port = 8888;
	settings.bindAddresses = [ "::1" , "127.0.0.1" ];
	settings.options = HTTPServerOption.parseFormBody | HTTPServerOption.parseURL;
	
	auto router = new URLRouter;

	router.any( "*" , &handle_http );
	router.any( "*" , handleWebSockets( &handle_websocket ) );

	listenHTTP( settings , router );
}

void handle_websocket( scope WebSocket sock )
{
	while( sock.connected ) try {

		auto message = parseJsonString( sock.receiveText );
		auto data = message[ "data" ];
		auto resp = DB.action( message[ "method" ].get!string , message[ "id" ].get!string , [] , data );

		sock.send( [
			"request_id" : message[ "request_id" ] ,
			"data" : resp ,
		].Json.toString );

	} catch( Exception error ) {
		sock.send( [ "error" : error.msg.Json ].Json.toString );
		stderr.writeln( error.msg );
		stderr.writeln( error.info );
	}
}

void handle_http( HTTPServerRequest req , HTTPServerResponse res )
{
	if( "Upgrade" in req.headers ) return;

	try {

		res.writeBody( DB.action( req.method.to!string , req.path[ 1 .. $ ] , [] , Json() ).toString , "application/json" );

	} catch( Exception error ) {
		res.writeBody( error.msg , HTTPStatus.internalServerError , "text/plain" );
		stderr.writeln( error.msg );
		stderr.writeln( error.info );
	}
}

class DB {

	static Json get( string id , string[] fetch )
	{
		auto link = Store.var( 1 , id.to!uint );
		auto comment = link.read!Comment;

		return [
			"id" : id.Json ,
			"parent" : comment.parent.offset.Json ,
			"message" : comment.message.read!string.Json ,
		].Json;
	}

	static Json patch( string id , Json data , string[] fetch )
	{
		auto link = Store.make!Comment( Store.var( 1 , "parent" in data ? data["parent"].get!uint : 0 ) , Store.var( data["message"].get!string ) );
		Store.commit( link );
		return [ "id" : link.offset.Json ].Json;
	}

	static Json action( string method , string id , string[] fetch , Json data )
	{
		switch( method ) {
			case "GET" : return DB.get( id , fetch );
			case "PATCH" : return DB.patch( id , data , fetch );
			default : throw new Exception( "Unknown method " ~ method );
		}
	}

}
