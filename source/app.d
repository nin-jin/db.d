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
	static Link* root;

	static auto stream( const ushort id )
	{
		while( id >= streams.length ) {
			streams ~= new Stream( path ~ streams.length.to!string ~ ".dbd" );
		}
		return streams[ id ];
	}

	static auto write( Record )( Record record )
	{
		return Link( 1 , stream(1).write( record ) );
	}

	static commit( Link root )
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
		
		stream(0).write( *Store.root );
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
	}

	Stream seek( uint offset )
	{
		this.input.seek( offset );
		return this;
	}

	auto write( Data )( Data data )
	{
		auto offset = this.output.tell.to!uint;
		this.put( data );
		return offset;
	}

	void sync()
	{
		this.output.flush;
		this.output.sync;
	}

}

void put( Data )( Stream stream , Data data )
if( !is( Data == struct ) && !isDynamicArray!Data )
{
	stream.output.lockingBinaryWriter.put( data );
}

void put( Data )( Stream stream , Data data )
if( is( Data == struct ) )
{
	foreach( ref value ; data.tupleof ) stream.put( value );
}

void put( Data )( Stream stream , Data data )
if( isDynamicArray!Data )
{
	foreach( item ; data ) stream.put( item );
}

void put( Data ... )( Stream stream , Data data )
if( data.length > 1 )
{
	foreach( item ; data ) stream.put( item );
}

auto read( Data )( Stream stream )
if( FieldNameTuple!Data.length <= 1 )
{
	Unqual!Data[1] buffer;
	stream.input.rawRead( buffer );
	return buffer[0];
}

Data read( Data )( Stream stream , ushort length )
if( isDynamicArray!Data )
{
	alias Element = ElementEncodingType!Data;
	auto buffer = new Unqual!Element[ length ];
	foreach( ref slot ; buffer ) slot = stream.read!Element;
	return cast(Data) buffer;
}

Data read( Data )( Stream stream )
if( isDynamicArray!Data )
{
	return stream.read!Data( stream.read!ushort );
}

auto read( Data )( Stream stream )
if( FieldNameTuple!Data.length > 1 )
{
	alias Args = FieldTypeTuple!Data;
	Args args;
	foreach( index , ref arg ; args ) arg = stream.read!( Args[ index ] );
	return Data( args );
}



struct Null {};

enum Type : ubyte
{
	Value = 0 ,
	Char = 1 ,
	Short = 2 ,
	Link = 3 ,
	Int = 4 ,
	Leaf = 5 ,
	Branch = 6 ,
	Long = 8 ,
}

enum Value : ubyte
{
	Null = 0 ,
	True = 1 ,
	False = 2 ,
}

struct Tag
{
	align(1)
	{
		Type type;
		ubyte value;
	}
}



alias Vary = Algebraic!( Null , bool , string , ushort[] , Link[] , uint[] , Leaf[] , Branch[] , ulong[] );

void put( Stream stream , Vary var )
{
	return var.visit!(
		( Null val )=> stream.put( Value.Null ) ,
		( bool val )=> stream.put( val ? Value.True : Value.False ) ,
		( string val )=> stream.put( Tag( Type.Char , val.length.to!ubyte ) , val ) ,
		( ushort[] val )=> stream.put( Tag( Type.Short , val.length.to!ubyte ) , val ) ,
		( Link[] val )=> stream.put( Tag( Type.Link , val.length.to!ubyte ) , val ) ,
		( uint[] val )=> stream.put( Tag( Type.Int , val.length.to!ubyte ) , val ) ,
		( Leaf[] val )=> stream.put( Tag( Type.Leaf , val.length.to!ubyte ) , val ) ,
		( Branch[] val )=> stream.put( Tag( Type.Branch , val.length.to!ubyte ) , val ) ,
		( ulong[] val )=> stream.put( Tag( Type.Long , val.length.to!ubyte ) , val ) ,
	);
}

auto read( Data : Vary )( Stream stream )
{
	const tag = stream.read!Tag;
	final switch( tag.type ) {
		case Type.Value :
			final switch( tag.value ) {
				case Value.Null : return Data( Null() );
				case Value.False : return Data( false );
				case Value.True : return Data( true );
			}
		case Type.Char : return Data( stream.read!string( tag.value.to!ushort ) );
		case Type.Short : return Data( stream.read!(ushort[])( tag.value.to!ushort ) );
		case Type.Link : return Data( stream.read!(Link[])( tag.value.to!ushort ) );
		case Type.Int : return Data( stream.read!(uint[])( tag.value.to!ushort ) );
		case Type.Leaf : return Data( stream.read!(Leaf[])( tag.value.to!ushort ) );
		case Type.Branch : return Data( stream.read!(Branch[])( tag.value.to!ushort ) );
		case Type.Long : return Data( stream.read!(ulong[])( tag.value.to!ushort ) );
	}
}

Vary select( Vary var , uint key )
{
	return var.tryVisit!(
		( Branch[] val )=> val.select( key ) ,
		( Leaf[] val )=> val.select( key ) ,
	);
}

Branch[] insert( Vary var , uint key , Vary value )
{
	return var.tryVisit!(
		( Branch[] val )=> val.insert( key , value ) ,
		( Leaf[] val )=> val.insert( key , value ) ,
	);
}



struct Link
{
	align(1)
	{
		ushort stream;
		uint offset;
	}

	Data read( Data )( )
	{
		return Store.stream( this.stream ).seek( this.offset ).read!Data;
	}
}

Vary select( Link dict , uint key )
{
	return dict.read!Vary.select( key );
}

Link insert( Link link , uint key , Vary value )
{
	auto branches = link.read!Vary.insert( key , value );
	switch( branches.length ) {
		case 1 : return branches[0].target;
		default : return Store.write( branches.Vary );
	}
}



uint hash( string data )
{
	auto res = cast(int[]) digest!( MurmurHash3!(128,64) )( data );
	return res[0];
}



struct Leaf
{
	align(4) {
		uint key;
		Vary value;
	}
}

Vary select( Leaf[] leafs , uint key )
{
	foreach( leaf ; leafs ) if( key == leaf.key ) return leaf.value;
	return Vary( Null() );
}

Branch[] insert( Leaf[] leafs , uint key , Vary value )
{
	Leaf[] leafs2 = null;

	foreach( index , ref leaf ; leafs ) {
		if( key > leaf.key ) continue;

		if( key == leaf.key ) {
			leafs2 = leafs[ 0 .. index ] ~ Leaf( key , value ) ~ leafs[ ( index + 1 ) .. $ ];
		} else {
			leafs2 = leafs[ 0 .. index ] ~ Leaf( key , value ) ~ leafs[ index .. $ ];
		}
		break;
	}

	if( leafs2 is null ) leafs2 = leafs ~ Leaf( key , value );

	if( leafs2.length < 5 ) return [ Branch( leafs2[ $ - 1 ].key , Store.write( Vary( leafs2 ) ) ) ];
	return leafs2.chunks(3).map!( chunk => Branch( chunk[ $ - 1 ].key , Store.write( Vary( chunk ) ) ) ).array;
}



struct Branch
{
	align(4) {
		uint key;
		Link target;
	}
}

Vary select( Branch[] branches , uint key )
{
	foreach( branch ; branches ) if( key <= branch.key ) return branch.target.select( key );
	return Vary( Null() );
}

Branch[] insert( Branch[] branches , uint key , Vary value )
{
	foreach( index , ref branch ; branches ) {
		if( index < branches.length - 1 && key > branch.key ) continue;

		branches = branches[ 0 .. index ] ~ branch.target.read!Vary.insert( key , value ) ~ branches[ ( index + 1 ) .. $ ];
		break;
	}

	if( branches.length < 5 ) return [ Branch( branches[ $ - 1 ].key , Store.write( Vary( branches ) ) ) ];
	return branches.chunks(3).map!( chunk => Branch( chunk[ $ - 1 ].key , Store.write( Vary( chunk ) ) ) ).array;
}



class DB
{

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

		DB.dict = Store.write( Vary( cast(Leaf[]) [] ) );
	}

	static void handle_websocket( scope WebSocket sock )
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

	static void handle_http( HTTPServerRequest req , HTTPServerResponse res )
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

	static Json get( string id , string[] fetch )
	{
		auto found = DB.dict.select( id.to!uint );
		if( found.peek!Null ) throw new Exception( "Not found: " ~ id.to!string );
		auto comment = found.peek!(Link[])[0][0].read!Vary;

		return [
			"id" : id.Json ,
			"parent" : comment.select( "parent".hash ).peek!(Link[])[0][0].read!Vary.peek!(uint[])[0][0].Json ,
			"message" : comment.select( "message".hash ).peek!(Link[])[0][0].read!Vary.peek!string[0].Json ,
		].Json;
	}

	static uint last_index;
	static Link dict;
	
	static Json patch( string id , Json data , string[] fetch )
	{
		auto link = Store.write( Vary( cast(Leaf[]) [] ) );
		if( "parent" in data ) link = link.insert( "parent".hash , Vary([ data["parent"].get!uint ]) );
		if( "message" in data ) link = link.insert( "message".hash , Vary([ Store.write( Vary( data["message"].get!string ) ) ]) );
		++ DB.last_index;
		DB.dict = DB.dict.insert( DB.last_index , Vary([ link ]) );

		Store.commit = DB.dict;
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

	/+
	static void vacuum()
	{
		Store.commit = DB.dict.copy( Store.stream( Store.streams.length ) );
	}
	+/

}

unittest
{
	auto dict = Store.write( Vary( new Leaf[0] ) );
	dict = dict.insert( 0 , "Hello0".Vary );
	dict = dict.insert( 1 , "Hello1".Vary );
	dict = dict.insert( 2 , "Hello2".Vary );
	dict = dict.insert( 3 , "Hello3".Vary );
	dict = dict.insert( 4 , "Hello4".Vary );
	dict = dict.insert( 5 , "Hello5".Vary );
	Store.commit = dict;

	assert( dict.select( 0 ).peek!string[0] == "Hello0" );
	assert( dict.select( 1 ).peek!string[0] == "Hello1" );
	assert( dict.select( 2 ).peek!string[0] == "Hello2" );
	assert( dict.select( 3 ).peek!string[0] == "Hello3" );
	assert( dict.select( 4 ).peek!string[0] == "Hello4" );
	assert( dict.select( 5 ).peek!string[0] == "Hello5" );
	assert( dict.select( 6 ).peek!Null );
}