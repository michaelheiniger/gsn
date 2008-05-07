package gsn.beans;

import gsn.utils.CaseInsensitiveComparator;
import java.io.Serializable;
import java.util.TreeMap;
import org.apache.log4j.Logger;

public final class StreamElement implements Serializable {

	private static final long                      serialVersionUID  = 2000261462783698617L;

	private static final transient Logger          logger            = Logger.getLogger( StreamElement.class );

	private transient TreeMap < String , Integer > indexedFieldNames = null;

	private long                                   timeStamp         = -1;

	private String [ ]                             fieldNames;

	private Serializable [ ]                       fieldValues;

	private Byte [ ]                               fieldTypes;

	private long                                   internalPrimayKey = -1;

	public StreamElement (StreamElement other) {
		this.fieldNames=new String[other.fieldNames.length];
		for (int i=0;i<other.fieldNames.length;i++) {
			fieldNames[i]=other.fieldNames[i];
			fieldValues[i]=other.fieldValues[i];
			fieldTypes[i]=other.fieldTypes[i];
		}
		this.timeStamp=other.timeStamp;
		this.internalPrimayKey = other.internalPrimayKey;
	}
	public StreamElement ( DataField [ ] outputStructure , final Serializable [ ] data  ) {
		this(outputStructure,data,System.currentTimeMillis());
	}
	public StreamElement ( DataField [ ] outputStructure , final Serializable [ ] data , final long timeStamp ) {
		this.fieldNames = new String [ outputStructure.length ];
		this.fieldTypes = new Byte [ outputStructure.length ];
		this.timeStamp = timeStamp;
		for ( int i = 0 ; i < this.fieldNames.length ; i++ ) {
			this.fieldNames[ i ] = outputStructure[ i ].getName( ).toLowerCase( );
			this.fieldTypes[ i ] = outputStructure[ i ].getDataTypeID( );
		}
		if ( this.fieldNames.length != data.length ) throw new IllegalArgumentException( "The length of dataFileNames and the actual data provided in the constructor of StreamElement doesn't match." );
		this.verifyTypesCompatibility( this.fieldTypes , data );
		this.fieldValues = data;
	}

	public StreamElement ( final String [ ] dataFieldNames , final Byte [ ] dataFieldTypes , final Serializable [ ] data ) {
		this( dataFieldNames , dataFieldTypes , data , System.currentTimeMillis( ) );
	}

	public StreamElement ( final String [ ] dataFieldNames , final Byte [ ] dataFieldTypes , final Serializable [ ] data , final long timeStamp ) {
		if ( dataFieldNames.length != dataFieldTypes.length )
			throw new IllegalArgumentException( "The length of dataFileNames and dataFileTypes provided in the constructor of StreamElement doesn't match." );
		if ( dataFieldNames.length != data.length ) throw new IllegalArgumentException( "The length of dataFileNames and the actual data provided in the constructor of StreamElement doesn't match." );
		this.timeStamp = timeStamp;
		this.fieldTypes = dataFieldTypes;
		this.fieldNames = dataFieldNames;
		this.verifyTypesCompatibility( dataFieldTypes , data );
		this.fieldValues = data;
	}

	private void verifyTypesCompatibility ( final Byte [ ] fieldTypes , final Serializable [ ] data ) throws IllegalArgumentException {
		for ( int i = 0 ; i < data.length ; i++ ) {
			if ( data[ i ] == null ) continue;
			switch ( fieldTypes[ i ] ) {
			case DataTypes.SMALLINT :
				if ( !( data[ i ] instanceof Short ) )
					throw new IllegalArgumentException( "The newly constructed Stream Element is not consistant. The " + ( i + 1 ) + "th field is defined as " + DataTypes.TYPE_NAMES[ fieldTypes[i] ]
					                                                                                                                                                                   + " while the actual data in the field is of type : *" + data[ i ].getClass( ).getCanonicalName( ) + "*" );
				break;
			case DataTypes.BIGINT :
				if ( !( data[ i ] instanceof Long ) ) { throw new IllegalArgumentException( "The newly constructed Stream Element is not consistant. The " + ( i + 1 ) + "th field is defined as "
						+ DataTypes.TYPE_NAMES[ fieldTypes[i] ] + " while the actual data in the field is of type : *" + data[ i ].getClass( ).getCanonicalName( ) + "*" ); }
				break;
			case DataTypes.CHAR :
			case DataTypes.VARCHAR :
				if ( !( data[ i ] instanceof String ) ) { throw new IllegalArgumentException( "The newly constructed Stream Element is not consistant. The " + ( i + 1 ) + "th field is defined as "
						+ DataTypes.TYPE_NAMES[ fieldTypes[i] ] + " while the actual data in the field is of type : *" + data[ i ].getClass( ).getCanonicalName( ) + "*" ); }
				break;
			case DataTypes.INTEGER :
				if ( !( data[ i ] instanceof Integer)) { throw new IllegalArgumentException( "The newly constructed Stream Element is not consistant. The " + ( i + 1 ) + "th field is defined as "
						+ DataTypes.TYPE_NAMES[ fieldTypes[i] ] + " while the actual data in the field is of type : *" + data[ i ].getClass( ).getCanonicalName( ) + "*" ); }
				break;
			case DataTypes.DOUBLE :
				if ( !( data[ i ] instanceof Double || data[ i ] instanceof Float ) )
					throw new IllegalArgumentException( "The newly constructed Stream Element is not consistant. The " + ( i + 1 ) + "th field is defined as " + DataTypes.TYPE_NAMES[ fieldTypes[i] ]
					                                                                                                                                                                   + " while the actual data in the field is of type : *" + data[ i ].getClass( ).getCanonicalName( ) + "*" );
				break;
			case DataTypes.BINARY :
				// if ( data[ i ] instanceof String ) data[ i ] = ( ( String )
				// data[ i ] ).getBytes( );
				if ( !( data[ i ] instanceof byte [ ] || data[ i ] instanceof String ) )
					throw new IllegalArgumentException( "The newly constructed Stream Element is not consistant. The " + ( i + 1 ) + "th field is defined as " + DataTypes.TYPE_NAMES[ fieldTypes[i] ]
					                                                                                                                                                                   + " while the actual data in the field is of type : *" + data[ i ].getClass( ).getCanonicalName( ) + "*" );
				break;
			}
		}
	}

	public String toString ( ) {
		final StringBuffer output = new StringBuffer( "timed = " );
		output.append( this.getTimeStamp( ) ).append( "\t" );
		for ( int i = 0 ; i < this.fieldNames.length ; i++ ) {
			output.append( "," ).append( this.fieldNames[ i ] ).append( "/" ).append( this.fieldTypes[ i ] ).append( " = " );
			if (fieldTypes[ i ] == DataTypes.BINARY) output.append( new String((byte[])fieldValues[ i ]) );
			else output.append( this.fieldValues[ i ] );
		}
		return output.toString( );
	}

	public final String [ ] getFieldNames ( ) {
		return this.fieldNames;
	}

	public final Byte [ ] getFieldTypes ( ) {
		return this.fieldTypes;
	}

	public final Serializable [ ] getData ( ) {
		return this.fieldValues;
	}

	public void setData (int index,Serializable data ) {
		this.fieldValues[index]=data;
	}

	public long getTimeStamp ( ) {
		return this.timeStamp;
	}

	public StringBuilder getFieldTypesInString ( ) {
		final StringBuilder stringBuilder = new StringBuilder( );
		for ( final byte i : this.getFieldTypes( ) )
			stringBuilder.append( DataTypes.TYPE_NAMES[ i ] ).append( " , " );
		return stringBuilder;
	}

	/**
	 * Returns true if the timestamp is valid. A timestamp is valid if it is
	 * above zero.
	 * 
	 * @return Whether the timestamp is valid or not.
	 */
	public boolean isTimestampSet ( ) {
		return this.timeStamp > 0;
	}

	/**
	 * Sets the time stamp of this stream element.
	 * 
	 * @param timeStamp The time stamp value. If the timestamp is zero or
	 * negative, it is considered non valid and zero will be placed.
	 */
	public void setTimeStamp ( long timeStamp ) {
		if ( this.timeStamp <= 0 )
			timeStamp = 0;
		else
			this.timeStamp = timeStamp;
	}

	/**
	 * This method gets the attribute name as the input and returns the value
	 * corresponding to that tuple.
	 * 
	 * @param fieldName The name of the tuple.
	 * @return The value corresponding to the named tuple.
	 */
	public final Serializable getData ( final String fieldName ) {
		if ( this.indexedFieldNames == null ) {
			this.indexedFieldNames = new TreeMap < String , Integer >( new CaseInsensitiveComparator( ) );
			for ( int i = 0 ; i < this.fieldNames.length ; i++ )
				this.indexedFieldNames.put( this.fieldNames[ i ] , i );
//			for (String k : this.indexedFieldNames.keySet())
//			System.out.println("Key : "+k + " VALUE = "+this.indexedFieldNames.get(k));
		}
//		System.out.print(fieldName+" AT INDEX : "+ this.indexedFieldNames.get( fieldName ) );
//		System.out.println(" HAS VALUE : "+this.fieldValues[ this.indexedFieldNames.get( fieldName ) ]);
		Integer index = this.indexedFieldNames.get( fieldName );
		if (index == null) {
			logger.info("There is a request for field "+fieldName+" for StreamElement: "+this.toString()+". As the requested field doesn't exist, GSN returns Null to the callee.");
			return null;
		}
		return this.fieldValues[ index ];
	}

	public long getInternalPrimayKey ( ) {
		return internalPrimayKey;
	}

	public void setInternalPrimayKey ( long internalPrimayKey ) {
		this.internalPrimayKey = internalPrimayKey;
	}

	/**
	 * @return
	 */
	public Object [ ] getDataInRPCFriendly ( ) {
		Object [ ] toReturn = new Object [ fieldValues.length ];
		for ( int i = 0 ; i < toReturn.length ; i++ ) {
			switch ( fieldTypes[ i ] ) {
			case DataTypes.DOUBLE :
				toReturn[ i ] = fieldValues[ i ];
				break;
			case DataTypes.BIGINT :
				toReturn[ i ] = Long.toString( ( Long ) fieldValues[ i ] );
				break;
//				case DataTypes.TIME :
//	toReturn[ i ] = Long.toString( ( Long ) fieldValues[ i ] );
//				break;
			case DataTypes.TINYINT :
			case DataTypes.SMALLINT :
			case DataTypes.INTEGER :
				toReturn[ i ] = new Integer( ( Integer ) fieldValues[ i ] );
				break;
			case DataTypes.CHAR :
			case DataTypes.VARCHAR :
			case DataTypes.BINARY :
				toReturn[ i ] = fieldValues[ i ];
				break;
			default :
				logger.error( "Type can't be converted : TypeID : " + fieldTypes[ i ] );
			}
		}
		return toReturn;

	}

	public static StreamElement createElementFromXMLRPC ( DataField [ ] outputFormat , Object [ ] fieldNames , Object [ ] fieldValues ,  long timestamp ) {
		Serializable [ ] values = new Serializable [ outputFormat.length ];
		for ( int i = 0 ; i < fieldNames.length ; i++ ) {
			switch ( findIndexInDataField( outputFormat , (String)fieldNames[i] ) ) {
			case DataTypes.DOUBLE :
				values[ i ] = ( Double ) fieldValues[ i ];
				break;
			case DataTypes.BIGINT :
//				case DataTypes.TIME :
				values[ i ] = Long.parseLong( ( String ) fieldValues[ i ] );
				break;
			case DataTypes.TINYINT :
				values[ i ] = Byte.parseByte( ( String ) fieldValues[ i ] );
				break;
			case DataTypes.SMALLINT :
			case DataTypes.INTEGER :
				values[ i ] = new Integer( ( Integer ) fieldValues[ i ] );
				break;
			case DataTypes.CHAR :
			case DataTypes.VARCHAR :
				values[ i ] = ( String ) fieldValues[ i ];
				break;
			case DataTypes.BINARY :
				values[ i ] = ( byte [ ] ) fieldValues[ i ];
				break;
			case -1:
			default :
				logger.error( "The field name doesn't exit in the output structure : FieldName : "+(String)fieldNames[i]   );
			}
		}
		return new StreamElement( outputFormat , values , timestamp );
	}
	/**
	 * Returns the type of the field in the output format or -1 if the field doesn't exit.
	 * @param outputFormat
	 * @param fieldName
	 * @return
	 */
	private static byte findIndexInDataField(DataField[] outputFormat, String fieldName) {
		for (int i=0;i<outputFormat.length;i++) 
			if (outputFormat[i].getName( ).equalsIgnoreCase( fieldName ))
				return outputFormat[i].getDataTypeID( );

		return -1;
	}
}
