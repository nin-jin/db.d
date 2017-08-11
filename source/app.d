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
	static sync_delay = 16.msecs;

	static Stream[] streams;

	static Var root;
	static Var draft;

	static auto stream( const ushort id )
	{
		while( id >= streams.length ) {
			streams ~= new Stream( streams.length.to!ushort );
		}
		return streams[ id ];
	}

	static commit( Var root )
	{
		draft = root;
		yield;
	}

	static sync()
	{
		if( root == draft ) return;
		
		if( streams.length > 0 ) {
			foreach( stream ; streams[ 1 .. $ ] ) stream.sync;
		}
		
		stream(0).put( draft );
		stream(0).sync;
		
		root = draft;
	}

	static this()
	{
		auto commits = stream(0);
		root = draft = ( commits.size < 8 ? Null : commits.seek( commits.size - Var.sizeof ).read!Var );

		runTask({
			while( true ) {
				sync_delay.sleep;
				sync;
			}
		});
	}

}

class Stream
{
	ushort id;

	File input;
	File output;

	this( ushort id )
	{
		this.id = id;
		auto path = Store.path ~ id.to!string ~ ".dbd";

		this.output = File( path , "ab" );
		this.input = File( path , "rb" );
	}

	uint put( Data ... )( Data data )
	{
		auto offset = this.size;
		
		auto writer = this.output.lockingBinaryWriter;

		foreach( item ; data ) writer.put( item );

		return offset;
	}

	auto size()
	{
		return this.output.tell.to!uint;
	}

	Stream seek( uint offset )
	{
		this.input.seek( offset );
		return this;
	}

	auto read( Type )()
	if( !isDynamicArray!Type )
	{
		Unqual!Type[1] buffer;
		this.input.rawRead( buffer );
		return cast( Type ) buffer[0];
	}

	auto read( Type )( ushort count )
	if( isDynamicArray!Type )
	{
		auto buffer = new Unqual!(ElementEncodingType!Type)[ count ];
		this.input.rawRead( buffer );
		return cast( Type ) buffer;
	}

	Type read( Type )()
	if( isDynamicArray!Type )
	{
		return this.read!Type( this.read!uint );
	}

	void sync()
	{
		this.output.flush;
		this.output.sync;
	}

}

enum Type : ubyte
{
	Null = 0 ,
	Char = 1 ,
	Int = 2 ,
	Leaf = 3 ,
	Branch = 4 ,
}

Type TypeOf( Data )()
{
	return staticIndexOf!( Unqual!Data , AliasSeq!( typeof(null) , char , uint , Leaf , Branch ) ).to!Type;
}

struct Var
{ align(1):

	Type type;
	ubyte count;

	union
	{
		struct
		{
			ushort stream;

			union
			{
				uint offset;
				uint value;
			}
		}

		char[6] data;

	}

	auto input()
	{
		return Store.stream( this.stream ).seek( this.offset );
	}

	this( Data )( Data[] data )
	{
		this.type = TypeOf!Data;
		if( data.length < 255 ) {
			this.count = data.length.to!ubyte;
			this.stream = 1;
			this.offset = Store.stream( 1 ).put( data );
		} else {
			this.stream = 1;
			this.offset = Store.stream( 1 ).put( data.length.to!ushort , data );
		}
	}

	this( Type type , ubyte count , ushort stream , uint offset )
	{
		this.type = type;
		this.count = count;
		this.stream = stream;
		this.offset = offset;
	}

	this( uint value )
	{
		this.type = Type.Int;
		this.count = 1;
		this.offset = value;
	}

	this( Leaf[] pairs )
	{
		if( pairs.length > 4 ) {
			this.type = type.Branch;
			this.count = 2;
			this.stream = 1;
			auto cut = pairs.length / 2;
			this.offset = Store.stream(1).put([
				Branch( pairs[ cut - 1 ].key , pairs[ 0 .. cut ].Var ) ,
				Branch( pairs[ $ - 1 ].key , pairs[ cut .. $ ].Var ) ,
			]);
		} else {
			this.type = type.Leaf;
			this.count = pairs.length.to!ubyte;
			this.stream = 1;
			this.offset = Store.stream(1).put( pairs );
		}
	}

	this( Branch[] pairs )
	{
		if( pairs.length == 1 ) {
			this = pairs[0].target;
		} else if( pairs.length > 4 ) {
			this.type = type.Branch;
			this.count = 2;
			this.stream = 1;
			auto cut = pairs.length / 2;
			pairs = [
				Branch( pairs[ cut - 1 ].key , pairs[ 0 .. cut ].Var ) ,
				Branch( pairs[ $ - 1 ].key , pairs[ cut .. $ ].Var ) ,
			];
		} else {
			this.type = Type.Branch;
			this.count = pairs.length.to!ubyte;
			this.stream = 1;
			this.offset = Store.stream(1).put( pairs );
		}
	}

	this( Var[] data )
	{
		auto leafs = new Leaf[ data.length ];

		foreach( index , item ; data ) {
			leafs[ index ] = Leaf( index.to!uint , item );
		}

		this( leafs );
	}

	this( Json data )
	{
		if( data.type == Json.Type.Int ) {
			this( data.get!uint );
		} else if( data.type == Json.Type.String ) {
			this( data.get!string );
		} else if( data.type == Json.Type.Array ) {
			this( data.get!(Json[]).map!( val => val.Var ).array );
		} else if( data.type == Json.Type.Object ) {
			Leaf[] leafs;
			foreach( key , value ; data.get!(Json[string]) ) {
				leafs ~= Leaf( key.hash[0] , value.Var );
			}
			this( leafs );
		} else {
			this(0);
			throw new Exception( "Unsupported JSON type " ~ data.type.to!string );
		}
	}

	Target to( Target )()
	if( is( Target : Json ) )
	{
		switch( this.type ) {
			case Type.Null : return null.Json;
			case Type.Char : return this.read!string.Json;
			case Type.Int : return this.value.Json;
			case Type.Leaf :
				auto res = Json.emptyArray;
				foreach( leaf ; this.read!(Leaf[]) ) {
					res ~= leaf.value.to!Json;
				}
				return res;
			case Type.Branch :
				auto res = Json.emptyArray;
				foreach( leaf ; this.read!(Branch[]) ) {
					res ~= leaf.target.to!Json;
				}
				return res;
			default : throw new Exception( "Can not convert this type to Json: " ~ this.type.to!string );
		}
	}

	auto read( Data )()
	if( !isArray!Data )
	{
		return this.input.read!( Data );
	}
	
	auto read( Data )()
	if( isArray!Data )
	{
		return this.input.read!( Data )( this.count );
	}
}



enum Null = Var( Type.Null , 0 , 0 , 0 );

Var select( Var var , uint key )
{
	switch( var.type ) {
		case Type.Leaf : return var.read!(Leaf[]).select( key );
		case Type.Branch : return var.read!(Branch[]).select( key );
		default : return var;
	}
}

Var select( Var var , uint[4] keys )
{
	foreach( key ; keys ) {
		if( var == Null ) return Null;
		var = var.select( key );
	}
	return var;
}

Branch[] insert( Var link , uint key , Var delegate( Var ) patch )
{
	switch( link.type ) {
		case Type.Null : return [ Branch( key , [ Leaf( key , patch( Null ) ) ].Var ) ];
		case Type.Leaf : return link.read!(Leaf[]).insert( key , patch );
		case Type.Branch : return link.read!(Branch[]).insert( key , patch );
		default : throw new Exception( "Wrong type for inserting: " ~ link.type.to!string );
	}
}

Var insert( Var var , uint[] keys , Var delegate( Var ) patch )
{
	auto patch_middle = ( keys.length == 1 ) ? patch : ( Var val )=> val.insert( keys[ 1 .. $ ] , patch );
	return var.insert( keys[0] , patch_middle ).Var;
}

uint next_key( Var link )
{
	switch( link.type ) {
		case Type.Null : return 0;
		case Type.Leaf : return link.read!(Leaf[])[ $ - 1 ].key + 1;
		case Type.Branch : return link.read!(Branch[])[ $ - 1 ].key + 1;
		default : throw new Exception( "Wrong type for next_key: " ~ link.type.to!string );
	}
}


uint[4] hash( string data )
{
	return cast(uint[4]) digest!( MurmurHash3!(128,64) )( data );
}

auto choose( Pair )( Pair[] pairs , uint key )
{
	foreach( index , pair ; pairs ) {
		if( key <= pair.key ) return index;
	}

	return pairs.length - 1;
}



struct Leaf
{ align(1):
	uint key;
	Var value;
}

Var select( const Leaf[] pairs , uint key )
{
	auto pair = pairs[ pairs.choose( key ) ];
	if( key == pair.key ) return pair.value;
	return Null;
}

Branch[] insert( const Leaf[] pairs , uint key , Var delegate( Var ) patch )
{
	if( ( key > pairs[ $ - 1 ].key ) ) {
		return ( pairs ~ [ Leaf( key , patch( Null ) ) ] ).reduce_pairs;
	}

	auto index = pairs.choose( key );

	if( pairs[ index ].key == key ) {
		return ( pairs[ 0 .. index ] ~ [ Leaf( key , patch( pairs[ index ].value ) ) ] ~ pairs[ index + 1 .. $ ] ).reduce_pairs;
	} else {
		return ( pairs[ 0 .. index ] ~ [ Leaf( key , patch( Null ) ) ] ~ pairs[ index .. $ ] ).reduce_pairs;
	}
}

Branch[] reduce_pairs( Pair )( Pair[] pairs )
{
	if( pairs.length < 5 ) return [ Branch( pairs[ $ - 1 ].key , pairs.Var ) ];
	return pairs.chunks(3).map!( chunk => Branch( chunk[ $ - 1 ].key , chunk.Var ) ).array;
}

/+
Branch[] insert( Leaf[] pairs , Json data )
{
	Leaf[ uint ] dict;

	foreach( pair ; pairs ) dict[ pair.key ] = pair;

	foreach( name , value ; data ) dict[ name.hash ] = value.Var;

	auto leafs = dict.sort!q{ a.key > b.key }.array;

	if( leafs.length < 5 ) return [ Branch( leafs[ $ - 1 ].key , leafs.Var ) ];
	return leafs.chunks(3).map!( chunk => Branch( chunk[ $ - 1 ].key , chunk.Var ) ).array;
}
+/


struct Branch
{ align(1):
	uint key;
	Var target;
}

Var select( const Branch[] pairs , uint key )
{
	return pairs[ pairs.choose( key ) ].target.select( key );
}

Branch[] insert( const Branch[] pairs , uint key , Var delegate( Var ) patch )
{
	auto index = pairs.choose( key );
	return ( pairs[ 0 .. index ] ~ pairs[ index ].target.insert( key , patch ) ~ pairs[ index + 1 .. $ ] ).reduce_pairs;
}

/+
Branch[] insert( Branch[] pairs , Json data )
{
	Json[ uint ] dict;

	foreach( name , value ; data ) {
		auto pair = pairs.select_pair( name.hash );

		if( pair.key !in dict ) dict[ pair.key ] = Json.emptyObject;
		dict[ pair.key ][ name ] = value.Var;
	}

	auto branches = pairs.map!q{ a.value.insert( dict[ a.key ] ) }.flat.array;

	if( leafs.length < 5 ) return [ Branch( leafs[ $ - 1 ].key , leafs.Var ) ];
	return leafs.chunks(3).map!( chunk => Branch( chunk[ $ - 1 ].key , chunk.Var ) ).array;
}
+/


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
		auto fetch = message["fetch"].get!(Json[]).map!q{ a.get!string }.array;
		auto resp = DB.action( message[ "method" ].get!string , message[ "id" ].get!string , fetch , data );

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

		res.writeBody( DB.action( req.method.to!string , req.path[ 1 .. $ ] , req.query.getAll( "fetch" ) , Json() ).toString , "application/json" );

	} catch( Exception error ) {
		res.writeBody( error.msg , HTTPStatus.internalServerError , "text/plain" );
		stderr.writeln( error.msg );
		stderr.writeln( error.info );
	}
}


// comment=123(parent,message,author)
// coment/* => parent/child

class DB {

	static Json get( string id , const string[] fetch )
	{
		auto entity = Store.root.select( id.to!uint );

		auto resp = [ "id" : id.Json ].Json;

		foreach( field ; fetch ) {
			resp[ field ] = entity.select( field.hash[0] ).to!Json;
		}

		return resp;
	}

	static Json patch( string id , Json data , const string[] fetch )
	{
		auto entity = data.Var;

		auto key = Store.draft.next_key;

		Store.commit = Store.draft.insert( key , val => entity ).Var;

		if( data["parent"].type == Json.Type.String ) {
			auto parent_id = data["parent"].get!string.to!uint;
			
			Store.commit = Store.draft.insert( parent_id , ( Var parent ) {
				return parent.insert( "child".hash[0] , ( Var child ) {
					return child.insert( child.next_key , val => key.Var ).Var;
				} ).Var;
			} ).Var;
		}
		
		return [ "id" : key.to!string.Json ].Json;
	}

	static Json action( string method , string id , const string[] fetch , Json data )
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
	dict = dict.insert( 0 , val => "Hello0".Var ).Var;
	dict = dict.insert( 1 , val => "Hello1".Var ).Var;
	dict = dict.insert( 2 , val => "Hello2".Var ).Var;
	dict = dict.insert( 3 , val => "Hello3".Var ).Var;
	dict = dict.insert( 4 , val => "Hello4".Var ).Var;
	dict = dict.insert( 5 , val => "Hello5".Var ).Var;

	assert( dict.select( 0 ).read!string == "Hello0" );
	assert( dict.select( 1 ).read!string == "Hello1" );
	assert( dict.select( 2 ).read!string == "Hello2" );
	assert( dict.select( 3 ).read!string == "Hello3" );
	assert( dict.select( 4 ).read!string == "Hello4" );
	assert( dict.select( 5 ).read!string == "Hello5" );
	assert( dict.select( 6 ) == Null );
}

unittest
{
	auto dict = [
		"Hello0".Json ,
		"Hello1".Json ,
		"Hello2".Json ,
		"Hello3".Json ,
		"Hello4".Json ,
		"Hello5".Json ,
	].Json.Var;

	assert( dict.select( 0 ).read!string == "Hello0" );
	assert( dict.select( 1 ).read!string == "Hello1" );
	assert( dict.select( 2 ).read!string == "Hello2" );
	assert( dict.select( 3 ).read!string == "Hello3" );
	assert( dict.select( 4 ).read!string == "Hello4" );
	assert( dict.select( 5 ).read!string == "Hello5" );
	assert( dict.select( 6 ) == Null );
}
