import vibe.core.core;
//import vibe.core.concurrency;
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

	static Var root;
	static Var draft;

	static auto stream( const ushort id )
	{
		while( id >= streams.length ) {
			streams ~= new Stream( streams.length.to!ushort );
		}
		return streams[ id ];
	}

	static void sync()
	{
		if( root == draft ) return;
		
		if( streams.length > 0 ) {
			foreach( stream ; streams[ 1 .. $ ] ) stream.sync;
		}
		
		stream(0).put( draft );
		stream(0).sync;
		
		root = draft;
	}

	static patch( Var delegate( Var ) task ) {
		Store.draft = task( draft );
	}

	static this()
	{
		auto commits = stream(0);
		root = draft = ( commits.size < 8 ? Null : commits.seek( commits.size - Var.sizeof.to!uint ).read!Var );

		runTask({
			while( true ) {
				1.msecs.sleep;
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
		if( count > 0 ) this.input.rawRead( buffer );
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
		if( data.type == Json.Type.Null ) {
			this( Type.Null , 0 , 0 , 0 );
		} else if( data.type == Json.Type.Int ) {
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
			this( leafs.sort!q{ a.key < b.key }.array );
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
		static if( typeid( Data ) == typeid( uint ) ) return this.value;
		else return this.input.read!( Data );
	}
	
	auto read( Data )()
	if( isArray!Data )
	{
		return this.input.read!( Data )( this.count );
	}
}



enum Null = Var( Type.Null , 0 , 0 , 0 );

Leaf[] leafs( Var var )
{
	switch( var.type ) {
		case Type.Leaf : return var.read!(Leaf[]);
		case Type.Branch :
			Leaf[] leafs;
			foreach( branch ; var.read!(Branch[]) ) {
				leafs ~= branch.target.leafs;
			}
			return leafs;
		default : throw new Exception( "Unsupported type for leafs: " ~ var.type );
	}
}

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

Var[ immutable string[] ] select( Var root , string[][] path )
{
	Var[ immutable string[] ] nodes = [ [] : root ];
	Var[ immutable string[] ] values;

	foreach( names ; path ) {

		Var[ immutable string[] ] nodes1;

		foreach( p , v ; nodes ) {
			if( v.type == Type.Char ) {
				auto link = v.read!string;
				values[ p ] = v;
				foreach( p2 , v2 ; root.select( link ) ) {
					nodes1[ p2 ] = v2;
				}
			} else {
				nodes1[ p ] = v;
			}
		}

		Var[ immutable string[] ] nodes2;

		foreach( name ; names ) {

			foreach( p , v ; nodes1 ) {
				if( v == Null ) continue;
		
				if( name == "@" || name == "" ) {
					foreach( leaf ; v.leafs ) {
						nodes2[ p ~ [ "@" ~ leaf.key.to!string ].idup ] = leaf.value;
					}
					continue;
				}

				auto p2 = p ~ [ name ].idup;

				auto cached = p2 in cache;
				if( cached ) {
					nodes2[ p2 ] = *cached;
					continue;
				}

				uint key;

				if( name[0] == '@' ) {
					key = name[ 1 .. $ ].to!uint;
				} else {
					key = name.hash[0];
				}

				auto v2 = v.select( key );
				nodes2[ p2 ] = v2;
				cache[ p2 ] = v2;
			}
		}

		nodes = nodes2;

	}

	foreach( p , v ; nodes ) {
		values[ p ] = v;
	}

	return values;
}

auto select( Var root , string path )
{
	return root.select( path.split("/").map!q{ a.split( "," ) }.array );
}

Var[ immutable string[] ] cache;

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

Var insert( Var link , string[] path , Var delegate( Var , string[] ) patch , string[] path2 = [] )
{
	if( path.length == 0 ) return patch( link , path2 );
	
	auto name = path[0];
	uint key;

	if( name[0] == '@' ) {
		if( name.length == 1 ) {
			key = link.next_key;
			name ~= key.to!string;
		} else {
			key = name[ 1 .. $ ].to!uint;
		}
	} else {
		key = name.hash[0];
	}

	path2 ~= name;

	return link.insert( key , ( var ) {
		auto v = var.insert( path[ 1 .. $ ] , patch , path2 );
		cache[ path2.idup ] = v;
		return v;
	} ).Var;
}

Var insert( Var link , string path , Var delegate( Var , string ) patch )
{
	return link.insert( path.split("/") , ( var , path2 )=> patch( var , path2.join("/") ) );
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



static this()
{
	auto settings = new HTTPServerSettings;
	settings.port = 8888;
	settings.bindAddresses = [ "::1" , "127.0.0.1" ];
	settings.options = HTTPServerOption.parseFormBody | HTTPServerOption.parseURL;// | HTTPServerOption.distribute;
	
	auto router = new URLRouter;

	router.any( "*" , &handle_http );
	router.any( "*" , handleWebSockets( &handle_websocket ) );

	listenHTTP( settings , router );
}

void handle_websocket( scope WebSocket sock )
{
	string[] output;
	auto root = Store.root;

	while( sock.connected ) {
		try {
		
			if( root != Store.root ) {

				foreach( msg ; output ) sock.send( msg );
				output = [];

				root = Store.root;
			}

			if( !sock.dataAvailableForRead && output.length > 0 ) {
				1.msecs.sleep;
				continue;
			}
		
			auto text = sock.receiveText;		

			auto message = parseJsonString( text );
			auto data = message[ "data" ];
			auto method = message[ "method" ].get!string;
			auto resp = DB.action( method , message[ "id" ].get!string , data );

			auto resp_str = [
				"request_id" : message[ "request_id" ] ,
				"data" : resp ,
			].Json.toString;

			switch( method ) {
				case "GET" : sock.send( resp_str ); break;
				case "PATCH" : output ~= resp_str; break;
				default : throw new Exception( "Unknown method " ~ method );
			}

		} catch( Exception error ) {
			sock.send( [ "error" : error.msg.Json ].Json.toString );
			stderr.writeln( error.msg );
			stderr.writeln( error.info );
		}
	}
}

void handle_http( HTTPServerRequest req , HTTPServerResponse res )
{
	if( "Upgrade" in req.headers ) return;

	try {

		res.writeBody( DB.action( req.method.to!string , req.path[ 1 .. $ ] , Json() ).toString , "application/json" );

	} catch( Exception error ) {
		res.writeBody( error.msg , HTTPStatus.internalServerError , "text/plain" );
		stderr.writeln( error.msg );
		stderr.writeln( error.info );
	}
}


// user/age/@1,@2,@5:@6,@18:/article.json
// comment/@123/parent,[child/@]/message,[author/name,ava].json
// comment/@id => parent/child/@
//
// commnet
//   @123
//     parent \comment/@23
//     child
//       @0 \comment/@223
//       @1 \comment/@323
//     author \user/@1
//   @23 message \Hello 23
//   @223 message \Hello 223
//   @323 message \Hello 323
// user
//   @1
//     name \Jin
//     ava \trollface.png
//
//{
//	"comment" : {
//		"@123" : {
//			"parent" : "comment/@23" ,
//			"child" : {
//				"@0" : "comment/@223" ,
//				"@1" : "comment/@323" ,
//			} ,
//			"author" : "user/@1"
//		},
//		"@23" : {
//			"message" : "Hello 23"
//		} ,
//		"@223" : {
//			"message" : "Hello 223"
//		},
//		"@323" : {
//			"message" : "Hello 323"
//		}
//	}
//}

class DB {

	static Json get( string path )
	{
		auto entities = Store.root.select( path );

		auto resp = Json.emptyObject;

		foreach( path , entity ; entities ) {
			auto dict = &resp;

			foreach( name ; path[ 0 .. $ - 1 ] ) {
				
				auto sub = name in *dict;
				if( sub ) {
					dict = sub;
				} else {
					(*dict)[ name ] = Json.emptyObject;
					dict = name in *dict;
				}
			}

			(*dict)[ path[ $ - 1 ] ] = entity.to!Json;
		}

		return resp;
	}

	static Json patch( string path , Json data )
	{
		auto entity = data.Var;
		string path2;

		Store.patch( ( root ) {
			root = root.insert( path , ( val , p ) { path2 = p ; return entity; } );

			if( data["parent"].type == Json.Type.String ) {
				auto parent_path = data["parent"].get!string;

				root = root.insert( parent_path ~ "/child/@" , ( val , p )=> path2.Var );
			}

			return root;
		} );
		
		return [ path : path2.Json ].Json;
	}

	static Json action( string method , string id , Json data )
	{
		switch( method ) {
			case "GET" : return DB.get( id );
			case "PATCH" : return DB.patch( id , data );
			default : throw new Exception( "Unknown method " ~ method );
		}
	}

}


/// Array store
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

	assert( dict.next_key == 6 );
	assert( dict.select( "@0" ).read!string == "Hello0" );
	assert( dict.select( "@1" ).read!string == "Hello1" );
	assert( dict.select( 2 ).read!string == "Hello2" );
	assert( dict.select( 3 ).read!string == "Hello3" );
	assert( dict.select( 4 ).read!string == "Hello4" );
	assert( dict.select( 5 ).read!string == "Hello5" );
	assert( dict.select( 6 ) == Null );
}

/// Array append
unittest
{
	auto dict = Null;
	dict = dict.insert( 0 , val => "Hello0".Var ).Var;
	dict = dict.insert( 1 , val => "Hello1".Var ).Var;
	dict = dict.insert( 2 , val => "Hello2".Var ).Var;
	dict = dict.insert( 3 , val => "Hello3".Var ).Var;
	dict = dict.insert( "@4" , ( val , p )=> "Hello4".Var );
	dict = dict.insert( "@" , ( val , p )=> "Hello5".Var );

	assert( dict.next_key == 6 );
	assert( dict.select( "@0" ).read!string == "Hello0" );
	assert( dict.select( "@1" ).read!string == "Hello1" );
	assert( dict.select( 2 ).read!string == "Hello2" );
	assert( dict.select( 3 ).read!string == "Hello3" );
	assert( dict.select( 4 ).read!string == "Hello4" );
	assert( dict.select( 5 ).read!string == "Hello5" );
	assert( dict.select( 6 ) == Null );
}

/// Dictionary store
unittest
{
	auto dict = [
		"Hello0" : 0.Json ,
		"Hello1" : 1.Json ,
		"Hello2" : 2.Json ,
		"Hello3" : 3.Json ,
		"Hello4" : 4.Json ,
		"Hello5" : 5.Json ,
	].Json.Var;

	assert( dict.select( "Hello0" ).read!uint == 0 );
	assert( dict.select( "Hello1" ).read!uint == 1 );
	assert( dict.select( "Hello2" ).read!uint == 2 );
	assert( dict.select( "Hello3" ).read!uint == 3 );
	assert( dict.select( "Hello4" ).read!uint == 4 );
	assert( dict.select( "Hello5" ).read!uint == 5 );
	assert( dict.select( "Hello6" ) == Null );
}

/// Dictionary insert
unittest
{
	auto dict = Null;
	dict = dict.insert( "Hello0" , ( val , p )=> 0.Var );
	dict = dict.insert( "Hello1" , ( val , p )=> 1.Var );
	dict = dict.insert( "Hello2" , ( val , p )=> 2.Var );
	dict = dict.insert( "Hello3" , ( val , p )=> 3.Var );
	dict = dict.insert( "Hello4" , ( val , p )=> 4.Var );
	dict = dict.insert( "Hello5" , ( val , p )=> 5.Var );

	assert( dict.select( "Hello0" ).read!uint == 0 );
	assert( dict.select( "Hello1" ).read!uint == 1 );
	assert( dict.select( "Hello2" ).read!uint == 2 );
	assert( dict.select( "Hello3" ).read!uint == 3 );
	assert( dict.select( "Hello4" ).read!uint == 4 );
	assert( dict.select( "Hello5" ).read!uint == 5 );
	assert( dict.select( "Hello6" ) == Null );
}
