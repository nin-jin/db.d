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
import std.exception;
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

	static auto put( Record )( Record record , ushort type = Var.Link )
	{
		return Var( 1 , stream(1).put( record ) , type );
	}

	static auto make( Record , Args... )( Args args )
	{
		return Store.put( Record( args ) );
	}

	static auto var( string data )
	{
		if( data.length > 6 ) return Store.put( data );
		else return Var( data );
	}

	static auto var( ushort stream , uint offset )
	{
		return Var( stream , offset );
	}

	static auto var( Pair[] pairs )
	{
		switch( pairs.length ) {
			case 1 :
				Pair[1] pairs2 = pairs[ 0 .. 1 ];
				return Store.put( pairs2 , Var.Pair1 );
			case 2 :
				Pair[2] pairs2 = pairs[ 0 .. 2 ];
				return Store.put( pairs2 , Var.Pair2 );
			case 3 :
				Pair[3] pairs2 = pairs[ 0 .. 3 ];
				return Store.put( pairs2 , Var.Pair3 );
			case 4 :
				Pair[4] pairs2 = pairs[ 0 .. 4 ];
				return Store.put( pairs2 , Var.Pair4 );
			case 5 :
				Pair[5] pairs2 = pairs[ 0 .. 5 ];
				return Store.put( pairs2 , Var.Pair5 );
			default : assert(0);
		}
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
	File input;
	File output;

	this( string path )
	{
		this.output = File( path , "ab" );
		this.input = File( path , "rb" );

		if( this.output.tell == 0 ) {
			this.output.lockingBinaryWriter.put( "jin-dbd\n" );
		}
	}

	uint put( Data )( Data data ) if( !isDynamicArray!Data )
	{
		auto offset = this.output.tell.to!uint;
		this.output.lockingBinaryWriter.put( data );
		return offset;
	}

	uint put( Data )( Data data ) if( isDynamicArray!Data )
	{
		auto offset = this.output.tell.to!uint;
		
		auto writer = this.output.lockingBinaryWriter;

		writer.put( data.length.to!uint );
		writer.put( data );

		auto pad = 4 - data.length % 4;
		if( pad != 4 ) writer.put( "    "[ 0 .. pad ] );

		return offset;
	}

	Stream seek( uint offset )
	{
		this.input.seek( offset );
		return this;
	}

	auto read( Type )() if( !isDynamicArray!Type )
	{
		Unqual!Type[1] buffer;
		this.input.rawRead( buffer );
		return buffer[0];
	}

	Type read( Type )() if( isDynamicArray!Type )
	{
		auto buffer = new Unqual!(ElementEncodingType!Type)[ this.read!uint ];
		this.input.rawRead( buffer );
		return assumeUnique( buffer );
	}

	void sync()
	{
		this.output.flush;
		this.output.sync;
	}

}

struct Var
{align(2):

	enum : ushort {
		String0 = 0 ,
		String1 = 1 ,
		String2 = 2 ,
		String3 = 3 ,
		String4 = 4 ,
		String5 = 5 ,
		String6 = 6 ,
		Link = 7 ,
		Pair1 = 8 ,
		Pair2 = 9 ,
		Pair3 = 10 ,
		Pair4 = 11 ,
		Pair5 = 12 ,
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

	this( ushort stream , uint offset , ushort type = Var.Link )
	{
		this.type = type;
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
				return "\"" ~ this.data[ 0 .. this.type ].to!string ~ "\"";
			
			case Link : return "Link(" ~ this.stream.to!string ~ "/" ~ this.offset.to!string ~ ")";
			case Pair1 : return "Pair1(" ~ this.stream.to!string ~ "/" ~ this.offset.to!string ~ ")" ~ this.read!(Pair[1]).to!string;
			case Pair2 : return "Pair2(" ~ this.stream.to!string ~ "/" ~ this.offset.to!string ~ ")" ~ this.read!(Pair[2]).to!string;
			case Pair3 : return "Pair3(" ~ this.stream.to!string ~ "/" ~ this.offset.to!string ~ ")" ~ this.read!(Pair[3]).to!string;
			case Pair4 : return "Pair3(" ~ this.stream.to!string ~ "/" ~ this.offset.to!string ~ ")" ~ this.read!(Pair[4]).to!string;
			case Pair5 : return "Pair3(" ~ this.stream.to!string ~ "/" ~ this.offset.to!string ~ ")" ~ this.read!(Pair[5]).to!string;

			default : assert(0);
		}
	}

}

ulong hash( string data )
{
	auto res = digest!( MurmurHash3!(128,64) )( data );
	return * cast(ulong*) cast(void*) &res;
}

enum Null = Store.var( 0,0 );

struct Pair
{ align(8):

	ulong key;
	Var value;

	string toString()
	{
		return this.key.to!string ~ ":" ~ this.value.to!string;
	}
}

Var select( Pairs )( Pairs pairs , ulong key )
if( isStaticArray!Pairs )
{
	foreach( pair ; pairs ) {
		switch( pair.value.type ) {
			case Var.Pair1 :
			case Var.Pair2 :
			case Var.Pair3 :
			case Var.Pair4 :
			case Var.Pair5 :
				if( key <= pair.key ) return pair.value.select( key );
				break;
			default :
				if( key == pair.key ) return pair.value;
		}
	}
	return Null;
}

Var select( Var link , ulong key )
{
	switch( link.type ) {
		case Var.Pair1 : return link.read!( Pair[1] ).select( key );
		case Var.Pair2 : return link.read!( Pair[2] ).select( key );
		case Var.Pair3 : return link.read!( Pair[3] ).select( key );
		case Var.Pair4 : return link.read!( Pair[4] ).select( key );
		case Var.Pair5 : return link.read!( Pair[5] ).select( key );
		default : assert(0);
	}
}

Var insert( Var link , ulong key , Var value )
{
	auto pairs = link.insert_inner( key , value );
	switch( pairs.length ) {
		case 1 :
			switch( pairs[0].value.type ) {
				case Var.Pair1 :
				case Var.Pair2 :
				case Var.Pair3 :
				case Var.Pair4 :
				case Var.Pair5 :
					return pairs[0].value;
				default : break;
			}
		case 2 :
		case 3 :
			return Store.var( pairs );
		default : assert(0);
	}
}

Pair[] insert_inner( Pairs )( Pairs pairs , ulong key , Var value )
if( isStaticArray!Pairs )
{
	Pair[] pairs2;

	search : foreach_reverse( index , ref pair ; pairs ) {

		switch( pair.value.type ) {
			case Var.Pair1 :
			case Var.Pair2 :
			case Var.Pair3 :
			case Var.Pair4 :
			case Var.Pair5 :
				if( index == pairs.length - 1 || key <= pair.key ) {
					auto add = pair.value.insert_inner( key , value );
					pairs2 = pairs[ 0 .. index ] ~ add ~ pairs[ ( index + 1 ) .. $ ];
					if( pairs2.length < 5 ) return [ Pair( pairs2[ $ - 1 ].key , Store.var( pairs2 ) ) ];
					return pairs2.chunks(3).map!( chunk => Pair( chunk[ $ - 1 ].key , Store.var( chunk ) ) ).array;
				}
				break;
			default :
				if( key == pair.key ) {
					pair.value = value;
					return [ Pair( pairs[ $ - 1 ].key , Store.var( pairs ) ) ];
				}
		}

	}

	pairs2 = pairs[ 0 .. $ ] ~ [ Pair( key , value ) ];
	if( pairs2.length < 5 ) return [ Pair( pairs2[ $ - 1 ].key , Store.var( pairs2 ) ) ];
	return pairs2.chunks(3).map!( chunk => Pair( chunk[ $ - 1 ].key , Store.var( chunk ) ) ).array;
}

Pair[] insert_inner( Var link , ulong key , Var value )
{
	switch( link.type ) {
		case Var.Pair1 : return link.read!( Pair[1] ).insert_inner( key , value );
		case Var.Pair2 : return link.read!( Pair[2] ).insert_inner( key , value );
		case Var.Pair3 : return link.read!( Pair[3] ).insert_inner( key , value );
		case Var.Pair4 : return link.read!( Pair[4] ).insert_inner( key , value );
		case Var.Pair5 : return link.read!( Pair[5] ).insert_inner( key , value );
		default :
			if( link == Null ) return [ Pair( key , value ) ];
			assert( 0 , "Unexpected Var type " ~ link.type.to!string );
	}
}

struct Comment
{
align(4):

	ulong parent;
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
		auto link = DB.dict.select( id.to!uint );
		auto comment = link.read!Comment;

		return [
			"id" : id.Json ,
			"parent" : comment.parent.Json ,
			"message" : comment.message.read!string.Json ,
		].Json;
	}

	static ulong last_index = 0;
	static Var dict = Null;
	
	static Json patch( string id , Json data , string[] fetch )
	{
		auto link = Store.make!Comment( "parent" in data ? data["parent"].get!ulong : 0 , Store.var( data["message"].get!string ) );
		++ DB.last_index;
		DB.dict = DB.dict.insert( DB.last_index , link );

		Store.commit( DB.dict );
		return [ "id" : DB.last_index.Json ].Json;
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

unittest
{
	auto dict = Null;
	dict = dict.insert( 0 , Store.var( "Hello0" ) );
	dict = dict.insert( 1 , Store.var( "Hello1" ) );
	dict = dict.insert( 2 , Store.var( "Hello2" ) );
	dict = dict.insert( 3 , Store.var( "Hello3" ) );
	dict = dict.insert( 4 , Store.var( "Hello4" ) );
	dict = dict.insert( 5 , Store.var( "Hello5" ) );
	Store.commit = dict;

	assert( dict.select( 0 ).data[] == "Hello0" );
	assert( dict.select( 1 ).data[] == "Hello1" );
	assert( dict.select( 2 ).data[] == "Hello2" );
	assert( dict.select( 3 ).data[] == "Hello3" );
	assert( dict.select( 4 ).data[] == "Hello4" );
	assert( dict.select( 5 ).data[] == "Hello4" );
	assert( dict.select( 6 ) == Null );
}