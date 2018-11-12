/**
 * Autogenerated by Avro
 *
 * DO NOT EDIT DIRECTLY
 */
package com.madhouse.dsp.avro;

import org.apache.avro.specific.SpecificData;
import org.apache.avro.message.BinaryMessageEncoder;
import org.apache.avro.message.BinaryMessageDecoder;
import org.apache.avro.message.SchemaStore;

@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public class ImpressionTrack extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  private static final long serialVersionUID = -604195360230614857L;
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"ImpressionTrack\",\"namespace\":\"com.madhouse.dsp.avro\",\"fields\":[{\"name\":\"time\",\"type\":\"long\"},{\"name\":\"ua\",\"type\":\"string\",\"default\":\"\"},{\"name\":\"ip\",\"type\":\"string\"},{\"name\":\"args\",\"type\":\"string\",\"default\":\"\"},{\"name\":\"status\",\"type\":\"int\"},{\"name\":\"bid\",\"type\":\"string\",\"default\":\"\"},{\"name\":\"bidid\",\"type\":\"string\"},{\"name\":\"mediaid\",\"type\":\"long\"},{\"name\":\"adspaceid\",\"type\":\"long\"},{\"name\":\"projectid\",\"type\":\"long\"},{\"name\":\"cid\",\"type\":\"long\"},{\"name\":\"crid\",\"type\":\"long\"},{\"name\":\"invalid\",\"type\":\"int\",\"default\":0},{\"name\":\"income\",\"type\":\"int\"},{\"name\":\"cost\",\"type\":\"int\"},{\"name\":\"location\",\"type\":\"string\"}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }

  private static SpecificData MODEL$ = new SpecificData();

  private static final BinaryMessageEncoder<ImpressionTrack> ENCODER =
      new BinaryMessageEncoder<ImpressionTrack>(MODEL$, SCHEMA$);

  private static final BinaryMessageDecoder<ImpressionTrack> DECODER =
      new BinaryMessageDecoder<ImpressionTrack>(MODEL$, SCHEMA$);

  /**
   * Return the BinaryMessageDecoder instance used by this class.
   */
  public static BinaryMessageDecoder<ImpressionTrack> getDecoder() {
    return DECODER;
  }

  /**
   * Create a new BinaryMessageDecoder instance for this class that uses the specified {@link SchemaStore}.
   * @param resolver a {@link SchemaStore} used to find schemas by fingerprint
   */
  public static BinaryMessageDecoder<ImpressionTrack> createDecoder(SchemaStore resolver) {
    return new BinaryMessageDecoder<ImpressionTrack>(MODEL$, SCHEMA$, resolver);
  }

  /** Serializes this ImpressionTrack to a ByteBuffer. */
  public java.nio.ByteBuffer toByteBuffer() throws java.io.IOException {
    return ENCODER.encode(this);
  }

  /** Deserializes a ImpressionTrack from a ByteBuffer. */
  public static ImpressionTrack fromByteBuffer(
      java.nio.ByteBuffer b) throws java.io.IOException {
    return DECODER.decode(b);
  }

  @Deprecated public long time;
  @Deprecated public java.lang.CharSequence ua;
  @Deprecated public java.lang.CharSequence ip;
  @Deprecated public java.lang.CharSequence args;
  @Deprecated public int status;
  @Deprecated public java.lang.CharSequence bid;
  @Deprecated public java.lang.CharSequence bidid;
  @Deprecated public long mediaid;
  @Deprecated public long adspaceid;
  @Deprecated public long projectid;
  @Deprecated public long cid;
  @Deprecated public long crid;
  @Deprecated public int invalid;
  @Deprecated public int income;
  @Deprecated public int cost;
  @Deprecated public java.lang.CharSequence location;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>.
   */
  public ImpressionTrack() {}

  /**
   * All-args constructor.
   * @param time The new value for time
   * @param ua The new value for ua
   * @param ip The new value for ip
   * @param args The new value for args
   * @param status The new value for status
   * @param bid The new value for bid
   * @param bidid The new value for bidid
   * @param mediaid The new value for mediaid
   * @param adspaceid The new value for adspaceid
   * @param projectid The new value for projectid
   * @param cid The new value for cid
   * @param crid The new value for crid
   * @param invalid The new value for invalid
   * @param income The new value for income
   * @param cost The new value for cost
   * @param location The new value for location
   */
  public ImpressionTrack(java.lang.Long time, java.lang.CharSequence ua, java.lang.CharSequence ip, java.lang.CharSequence args, java.lang.Integer status, java.lang.CharSequence bid, java.lang.CharSequence bidid, java.lang.Long mediaid, java.lang.Long adspaceid, java.lang.Long projectid, java.lang.Long cid, java.lang.Long crid, java.lang.Integer invalid, java.lang.Integer income, java.lang.Integer cost, java.lang.CharSequence location) {
    this.time = time;
    this.ua = ua;
    this.ip = ip;
    this.args = args;
    this.status = status;
    this.bid = bid;
    this.bidid = bidid;
    this.mediaid = mediaid;
    this.adspaceid = adspaceid;
    this.projectid = projectid;
    this.cid = cid;
    this.crid = crid;
    this.invalid = invalid;
    this.income = income;
    this.cost = cost;
    this.location = location;
  }

  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call.
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return time;
    case 1: return ua;
    case 2: return ip;
    case 3: return args;
    case 4: return status;
    case 5: return bid;
    case 6: return bidid;
    case 7: return mediaid;
    case 8: return adspaceid;
    case 9: return projectid;
    case 10: return cid;
    case 11: return crid;
    case 12: return invalid;
    case 13: return income;
    case 14: return cost;
    case 15: return location;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  // Used by DatumReader.  Applications should not call.
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: time = (java.lang.Long)value$; break;
    case 1: ua = (java.lang.CharSequence)value$; break;
    case 2: ip = (java.lang.CharSequence)value$; break;
    case 3: args = (java.lang.CharSequence)value$; break;
    case 4: status = (java.lang.Integer)value$; break;
    case 5: bid = (java.lang.CharSequence)value$; break;
    case 6: bidid = (java.lang.CharSequence)value$; break;
    case 7: mediaid = (java.lang.Long)value$; break;
    case 8: adspaceid = (java.lang.Long)value$; break;
    case 9: projectid = (java.lang.Long)value$; break;
    case 10: cid = (java.lang.Long)value$; break;
    case 11: crid = (java.lang.Long)value$; break;
    case 12: invalid = (java.lang.Integer)value$; break;
    case 13: income = (java.lang.Integer)value$; break;
    case 14: cost = (java.lang.Integer)value$; break;
    case 15: location = (java.lang.CharSequence)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'time' field.
   * @return The value of the 'time' field.
   */
  public java.lang.Long getTime() {
    return time;
  }

  /**
   * Sets the value of the 'time' field.
   * @param value the value to set.
   */
  public void setTime(java.lang.Long value) {
    this.time = value;
  }

  /**
   * Gets the value of the 'ua' field.
   * @return The value of the 'ua' field.
   */
  public java.lang.CharSequence getUa() {
    return ua;
  }

  /**
   * Sets the value of the 'ua' field.
   * @param value the value to set.
   */
  public void setUa(java.lang.CharSequence value) {
    this.ua = value;
  }

  /**
   * Gets the value of the 'ip' field.
   * @return The value of the 'ip' field.
   */
  public java.lang.CharSequence getIp() {
    return ip;
  }

  /**
   * Sets the value of the 'ip' field.
   * @param value the value to set.
   */
  public void setIp(java.lang.CharSequence value) {
    this.ip = value;
  }

  /**
   * Gets the value of the 'args' field.
   * @return The value of the 'args' field.
   */
  public java.lang.CharSequence getArgs() {
    return args;
  }

  /**
   * Sets the value of the 'args' field.
   * @param value the value to set.
   */
  public void setArgs(java.lang.CharSequence value) {
    this.args = value;
  }

  /**
   * Gets the value of the 'status' field.
   * @return The value of the 'status' field.
   */
  public java.lang.Integer getStatus() {
    return status;
  }

  /**
   * Sets the value of the 'status' field.
   * @param value the value to set.
   */
  public void setStatus(java.lang.Integer value) {
    this.status = value;
  }

  /**
   * Gets the value of the 'bid' field.
   * @return The value of the 'bid' field.
   */
  public java.lang.CharSequence getBid() {
    return bid;
  }

  /**
   * Sets the value of the 'bid' field.
   * @param value the value to set.
   */
  public void setBid(java.lang.CharSequence value) {
    this.bid = value;
  }

  /**
   * Gets the value of the 'bidid' field.
   * @return The value of the 'bidid' field.
   */
  public java.lang.CharSequence getBidid() {
    return bidid;
  }

  /**
   * Sets the value of the 'bidid' field.
   * @param value the value to set.
   */
  public void setBidid(java.lang.CharSequence value) {
    this.bidid = value;
  }

  /**
   * Gets the value of the 'mediaid' field.
   * @return The value of the 'mediaid' field.
   */
  public java.lang.Long getMediaid() {
    return mediaid;
  }

  /**
   * Sets the value of the 'mediaid' field.
   * @param value the value to set.
   */
  public void setMediaid(java.lang.Long value) {
    this.mediaid = value;
  }

  /**
   * Gets the value of the 'adspaceid' field.
   * @return The value of the 'adspaceid' field.
   */
  public java.lang.Long getAdspaceid() {
    return adspaceid;
  }

  /**
   * Sets the value of the 'adspaceid' field.
   * @param value the value to set.
   */
  public void setAdspaceid(java.lang.Long value) {
    this.adspaceid = value;
  }

  /**
   * Gets the value of the 'projectid' field.
   * @return The value of the 'projectid' field.
   */
  public java.lang.Long getProjectid() {
    return projectid;
  }

  /**
   * Sets the value of the 'projectid' field.
   * @param value the value to set.
   */
  public void setProjectid(java.lang.Long value) {
    this.projectid = value;
  }

  /**
   * Gets the value of the 'cid' field.
   * @return The value of the 'cid' field.
   */
  public java.lang.Long getCid() {
    return cid;
  }

  /**
   * Sets the value of the 'cid' field.
   * @param value the value to set.
   */
  public void setCid(java.lang.Long value) {
    this.cid = value;
  }

  /**
   * Gets the value of the 'crid' field.
   * @return The value of the 'crid' field.
   */
  public java.lang.Long getCrid() {
    return crid;
  }

  /**
   * Sets the value of the 'crid' field.
   * @param value the value to set.
   */
  public void setCrid(java.lang.Long value) {
    this.crid = value;
  }

  /**
   * Gets the value of the 'invalid' field.
   * @return The value of the 'invalid' field.
   */
  public java.lang.Integer getInvalid() {
    return invalid;
  }

  /**
   * Sets the value of the 'invalid' field.
   * @param value the value to set.
   */
  public void setInvalid(java.lang.Integer value) {
    this.invalid = value;
  }

  /**
   * Gets the value of the 'income' field.
   * @return The value of the 'income' field.
   */
  public java.lang.Integer getIncome() {
    return income;
  }

  /**
   * Sets the value of the 'income' field.
   * @param value the value to set.
   */
  public void setIncome(java.lang.Integer value) {
    this.income = value;
  }

  /**
   * Gets the value of the 'cost' field.
   * @return The value of the 'cost' field.
   */
  public java.lang.Integer getCost() {
    return cost;
  }

  /**
   * Sets the value of the 'cost' field.
   * @param value the value to set.
   */
  public void setCost(java.lang.Integer value) {
    this.cost = value;
  }

  /**
   * Gets the value of the 'location' field.
   * @return The value of the 'location' field.
   */
  public java.lang.CharSequence getLocation() {
    return location;
  }

  /**
   * Sets the value of the 'location' field.
   * @param value the value to set.
   */
  public void setLocation(java.lang.CharSequence value) {
    this.location = value;
  }

  /**
   * Creates a new ImpressionTrack RecordBuilder.
   * @return A new ImpressionTrack RecordBuilder
   */
  public static com.madhouse.dsp.avro.ImpressionTrack.Builder newBuilder() {
    return new com.madhouse.dsp.avro.ImpressionTrack.Builder();
  }

  /**
   * Creates a new ImpressionTrack RecordBuilder by copying an existing Builder.
   * @param other The existing builder to copy.
   * @return A new ImpressionTrack RecordBuilder
   */
  public static com.madhouse.dsp.avro.ImpressionTrack.Builder newBuilder(com.madhouse.dsp.avro.ImpressionTrack.Builder other) {
    return new com.madhouse.dsp.avro.ImpressionTrack.Builder(other);
  }

  /**
   * Creates a new ImpressionTrack RecordBuilder by copying an existing ImpressionTrack instance.
   * @param other The existing instance to copy.
   * @return A new ImpressionTrack RecordBuilder
   */
  public static com.madhouse.dsp.avro.ImpressionTrack.Builder newBuilder(com.madhouse.dsp.avro.ImpressionTrack other) {
    return new com.madhouse.dsp.avro.ImpressionTrack.Builder(other);
  }

  /**
   * RecordBuilder for ImpressionTrack instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<ImpressionTrack>
    implements org.apache.avro.data.RecordBuilder<ImpressionTrack> {

    private long time;
    private java.lang.CharSequence ua;
    private java.lang.CharSequence ip;
    private java.lang.CharSequence args;
    private int status;
    private java.lang.CharSequence bid;
    private java.lang.CharSequence bidid;
    private long mediaid;
    private long adspaceid;
    private long projectid;
    private long cid;
    private long crid;
    private int invalid;
    private int income;
    private int cost;
    private java.lang.CharSequence location;

    /** Creates a new Builder */
    private Builder() {
      super(SCHEMA$);
    }

    /**
     * Creates a Builder by copying an existing Builder.
     * @param other The existing Builder to copy.
     */
    private Builder(com.madhouse.dsp.avro.ImpressionTrack.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.time)) {
        this.time = data().deepCopy(fields()[0].schema(), other.time);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.ua)) {
        this.ua = data().deepCopy(fields()[1].schema(), other.ua);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.ip)) {
        this.ip = data().deepCopy(fields()[2].schema(), other.ip);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.args)) {
        this.args = data().deepCopy(fields()[3].schema(), other.args);
        fieldSetFlags()[3] = true;
      }
      if (isValidValue(fields()[4], other.status)) {
        this.status = data().deepCopy(fields()[4].schema(), other.status);
        fieldSetFlags()[4] = true;
      }
      if (isValidValue(fields()[5], other.bid)) {
        this.bid = data().deepCopy(fields()[5].schema(), other.bid);
        fieldSetFlags()[5] = true;
      }
      if (isValidValue(fields()[6], other.bidid)) {
        this.bidid = data().deepCopy(fields()[6].schema(), other.bidid);
        fieldSetFlags()[6] = true;
      }
      if (isValidValue(fields()[7], other.mediaid)) {
        this.mediaid = data().deepCopy(fields()[7].schema(), other.mediaid);
        fieldSetFlags()[7] = true;
      }
      if (isValidValue(fields()[8], other.adspaceid)) {
        this.adspaceid = data().deepCopy(fields()[8].schema(), other.adspaceid);
        fieldSetFlags()[8] = true;
      }
      if (isValidValue(fields()[9], other.projectid)) {
        this.projectid = data().deepCopy(fields()[9].schema(), other.projectid);
        fieldSetFlags()[9] = true;
      }
      if (isValidValue(fields()[10], other.cid)) {
        this.cid = data().deepCopy(fields()[10].schema(), other.cid);
        fieldSetFlags()[10] = true;
      }
      if (isValidValue(fields()[11], other.crid)) {
        this.crid = data().deepCopy(fields()[11].schema(), other.crid);
        fieldSetFlags()[11] = true;
      }
      if (isValidValue(fields()[12], other.invalid)) {
        this.invalid = data().deepCopy(fields()[12].schema(), other.invalid);
        fieldSetFlags()[12] = true;
      }
      if (isValidValue(fields()[13], other.income)) {
        this.income = data().deepCopy(fields()[13].schema(), other.income);
        fieldSetFlags()[13] = true;
      }
      if (isValidValue(fields()[14], other.cost)) {
        this.cost = data().deepCopy(fields()[14].schema(), other.cost);
        fieldSetFlags()[14] = true;
      }
      if (isValidValue(fields()[15], other.location)) {
        this.location = data().deepCopy(fields()[15].schema(), other.location);
        fieldSetFlags()[15] = true;
      }
    }

    /**
     * Creates a Builder by copying an existing ImpressionTrack instance
     * @param other The existing instance to copy.
     */
    private Builder(com.madhouse.dsp.avro.ImpressionTrack other) {
            super(SCHEMA$);
      if (isValidValue(fields()[0], other.time)) {
        this.time = data().deepCopy(fields()[0].schema(), other.time);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.ua)) {
        this.ua = data().deepCopy(fields()[1].schema(), other.ua);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.ip)) {
        this.ip = data().deepCopy(fields()[2].schema(), other.ip);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.args)) {
        this.args = data().deepCopy(fields()[3].schema(), other.args);
        fieldSetFlags()[3] = true;
      }
      if (isValidValue(fields()[4], other.status)) {
        this.status = data().deepCopy(fields()[4].schema(), other.status);
        fieldSetFlags()[4] = true;
      }
      if (isValidValue(fields()[5], other.bid)) {
        this.bid = data().deepCopy(fields()[5].schema(), other.bid);
        fieldSetFlags()[5] = true;
      }
      if (isValidValue(fields()[6], other.bidid)) {
        this.bidid = data().deepCopy(fields()[6].schema(), other.bidid);
        fieldSetFlags()[6] = true;
      }
      if (isValidValue(fields()[7], other.mediaid)) {
        this.mediaid = data().deepCopy(fields()[7].schema(), other.mediaid);
        fieldSetFlags()[7] = true;
      }
      if (isValidValue(fields()[8], other.adspaceid)) {
        this.adspaceid = data().deepCopy(fields()[8].schema(), other.adspaceid);
        fieldSetFlags()[8] = true;
      }
      if (isValidValue(fields()[9], other.projectid)) {
        this.projectid = data().deepCopy(fields()[9].schema(), other.projectid);
        fieldSetFlags()[9] = true;
      }
      if (isValidValue(fields()[10], other.cid)) {
        this.cid = data().deepCopy(fields()[10].schema(), other.cid);
        fieldSetFlags()[10] = true;
      }
      if (isValidValue(fields()[11], other.crid)) {
        this.crid = data().deepCopy(fields()[11].schema(), other.crid);
        fieldSetFlags()[11] = true;
      }
      if (isValidValue(fields()[12], other.invalid)) {
        this.invalid = data().deepCopy(fields()[12].schema(), other.invalid);
        fieldSetFlags()[12] = true;
      }
      if (isValidValue(fields()[13], other.income)) {
        this.income = data().deepCopy(fields()[13].schema(), other.income);
        fieldSetFlags()[13] = true;
      }
      if (isValidValue(fields()[14], other.cost)) {
        this.cost = data().deepCopy(fields()[14].schema(), other.cost);
        fieldSetFlags()[14] = true;
      }
      if (isValidValue(fields()[15], other.location)) {
        this.location = data().deepCopy(fields()[15].schema(), other.location);
        fieldSetFlags()[15] = true;
      }
    }

    /**
      * Gets the value of the 'time' field.
      * @return The value.
      */
    public java.lang.Long getTime() {
      return time;
    }

    /**
      * Sets the value of the 'time' field.
      * @param value The value of 'time'.
      * @return This builder.
      */
    public com.madhouse.dsp.avro.ImpressionTrack.Builder setTime(long value) {
      validate(fields()[0], value);
      this.time = value;
      fieldSetFlags()[0] = true;
      return this;
    }

    /**
      * Checks whether the 'time' field has been set.
      * @return True if the 'time' field has been set, false otherwise.
      */
    public boolean hasTime() {
      return fieldSetFlags()[0];
    }


    /**
      * Clears the value of the 'time' field.
      * @return This builder.
      */
    public com.madhouse.dsp.avro.ImpressionTrack.Builder clearTime() {
      fieldSetFlags()[0] = false;
      return this;
    }

    /**
      * Gets the value of the 'ua' field.
      * @return The value.
      */
    public java.lang.CharSequence getUa() {
      return ua;
    }

    /**
      * Sets the value of the 'ua' field.
      * @param value The value of 'ua'.
      * @return This builder.
      */
    public com.madhouse.dsp.avro.ImpressionTrack.Builder setUa(java.lang.CharSequence value) {
      validate(fields()[1], value);
      this.ua = value;
      fieldSetFlags()[1] = true;
      return this;
    }

    /**
      * Checks whether the 'ua' field has been set.
      * @return True if the 'ua' field has been set, false otherwise.
      */
    public boolean hasUa() {
      return fieldSetFlags()[1];
    }


    /**
      * Clears the value of the 'ua' field.
      * @return This builder.
      */
    public com.madhouse.dsp.avro.ImpressionTrack.Builder clearUa() {
      ua = null;
      fieldSetFlags()[1] = false;
      return this;
    }

    /**
      * Gets the value of the 'ip' field.
      * @return The value.
      */
    public java.lang.CharSequence getIp() {
      return ip;
    }

    /**
      * Sets the value of the 'ip' field.
      * @param value The value of 'ip'.
      * @return This builder.
      */
    public com.madhouse.dsp.avro.ImpressionTrack.Builder setIp(java.lang.CharSequence value) {
      validate(fields()[2], value);
      this.ip = value;
      fieldSetFlags()[2] = true;
      return this;
    }

    /**
      * Checks whether the 'ip' field has been set.
      * @return True if the 'ip' field has been set, false otherwise.
      */
    public boolean hasIp() {
      return fieldSetFlags()[2];
    }


    /**
      * Clears the value of the 'ip' field.
      * @return This builder.
      */
    public com.madhouse.dsp.avro.ImpressionTrack.Builder clearIp() {
      ip = null;
      fieldSetFlags()[2] = false;
      return this;
    }

    /**
      * Gets the value of the 'args' field.
      * @return The value.
      */
    public java.lang.CharSequence getArgs() {
      return args;
    }

    /**
      * Sets the value of the 'args' field.
      * @param value The value of 'args'.
      * @return This builder.
      */
    public com.madhouse.dsp.avro.ImpressionTrack.Builder setArgs(java.lang.CharSequence value) {
      validate(fields()[3], value);
      this.args = value;
      fieldSetFlags()[3] = true;
      return this;
    }

    /**
      * Checks whether the 'args' field has been set.
      * @return True if the 'args' field has been set, false otherwise.
      */
    public boolean hasArgs() {
      return fieldSetFlags()[3];
    }


    /**
      * Clears the value of the 'args' field.
      * @return This builder.
      */
    public com.madhouse.dsp.avro.ImpressionTrack.Builder clearArgs() {
      args = null;
      fieldSetFlags()[3] = false;
      return this;
    }

    /**
      * Gets the value of the 'status' field.
      * @return The value.
      */
    public java.lang.Integer getStatus() {
      return status;
    }

    /**
      * Sets the value of the 'status' field.
      * @param value The value of 'status'.
      * @return This builder.
      */
    public com.madhouse.dsp.avro.ImpressionTrack.Builder setStatus(int value) {
      validate(fields()[4], value);
      this.status = value;
      fieldSetFlags()[4] = true;
      return this;
    }

    /**
      * Checks whether the 'status' field has been set.
      * @return True if the 'status' field has been set, false otherwise.
      */
    public boolean hasStatus() {
      return fieldSetFlags()[4];
    }


    /**
      * Clears the value of the 'status' field.
      * @return This builder.
      */
    public com.madhouse.dsp.avro.ImpressionTrack.Builder clearStatus() {
      fieldSetFlags()[4] = false;
      return this;
    }

    /**
      * Gets the value of the 'bid' field.
      * @return The value.
      */
    public java.lang.CharSequence getBid() {
      return bid;
    }

    /**
      * Sets the value of the 'bid' field.
      * @param value The value of 'bid'.
      * @return This builder.
      */
    public com.madhouse.dsp.avro.ImpressionTrack.Builder setBid(java.lang.CharSequence value) {
      validate(fields()[5], value);
      this.bid = value;
      fieldSetFlags()[5] = true;
      return this;
    }

    /**
      * Checks whether the 'bid' field has been set.
      * @return True if the 'bid' field has been set, false otherwise.
      */
    public boolean hasBid() {
      return fieldSetFlags()[5];
    }


    /**
      * Clears the value of the 'bid' field.
      * @return This builder.
      */
    public com.madhouse.dsp.avro.ImpressionTrack.Builder clearBid() {
      bid = null;
      fieldSetFlags()[5] = false;
      return this;
    }

    /**
      * Gets the value of the 'bidid' field.
      * @return The value.
      */
    public java.lang.CharSequence getBidid() {
      return bidid;
    }

    /**
      * Sets the value of the 'bidid' field.
      * @param value The value of 'bidid'.
      * @return This builder.
      */
    public com.madhouse.dsp.avro.ImpressionTrack.Builder setBidid(java.lang.CharSequence value) {
      validate(fields()[6], value);
      this.bidid = value;
      fieldSetFlags()[6] = true;
      return this;
    }

    /**
      * Checks whether the 'bidid' field has been set.
      * @return True if the 'bidid' field has been set, false otherwise.
      */
    public boolean hasBidid() {
      return fieldSetFlags()[6];
    }


    /**
      * Clears the value of the 'bidid' field.
      * @return This builder.
      */
    public com.madhouse.dsp.avro.ImpressionTrack.Builder clearBidid() {
      bidid = null;
      fieldSetFlags()[6] = false;
      return this;
    }

    /**
      * Gets the value of the 'mediaid' field.
      * @return The value.
      */
    public java.lang.Long getMediaid() {
      return mediaid;
    }

    /**
      * Sets the value of the 'mediaid' field.
      * @param value The value of 'mediaid'.
      * @return This builder.
      */
    public com.madhouse.dsp.avro.ImpressionTrack.Builder setMediaid(long value) {
      validate(fields()[7], value);
      this.mediaid = value;
      fieldSetFlags()[7] = true;
      return this;
    }

    /**
      * Checks whether the 'mediaid' field has been set.
      * @return True if the 'mediaid' field has been set, false otherwise.
      */
    public boolean hasMediaid() {
      return fieldSetFlags()[7];
    }


    /**
      * Clears the value of the 'mediaid' field.
      * @return This builder.
      */
    public com.madhouse.dsp.avro.ImpressionTrack.Builder clearMediaid() {
      fieldSetFlags()[7] = false;
      return this;
    }

    /**
      * Gets the value of the 'adspaceid' field.
      * @return The value.
      */
    public java.lang.Long getAdspaceid() {
      return adspaceid;
    }

    /**
      * Sets the value of the 'adspaceid' field.
      * @param value The value of 'adspaceid'.
      * @return This builder.
      */
    public com.madhouse.dsp.avro.ImpressionTrack.Builder setAdspaceid(long value) {
      validate(fields()[8], value);
      this.adspaceid = value;
      fieldSetFlags()[8] = true;
      return this;
    }

    /**
      * Checks whether the 'adspaceid' field has been set.
      * @return True if the 'adspaceid' field has been set, false otherwise.
      */
    public boolean hasAdspaceid() {
      return fieldSetFlags()[8];
    }


    /**
      * Clears the value of the 'adspaceid' field.
      * @return This builder.
      */
    public com.madhouse.dsp.avro.ImpressionTrack.Builder clearAdspaceid() {
      fieldSetFlags()[8] = false;
      return this;
    }

    /**
      * Gets the value of the 'projectid' field.
      * @return The value.
      */
    public java.lang.Long getProjectid() {
      return projectid;
    }

    /**
      * Sets the value of the 'projectid' field.
      * @param value The value of 'projectid'.
      * @return This builder.
      */
    public com.madhouse.dsp.avro.ImpressionTrack.Builder setProjectid(long value) {
      validate(fields()[9], value);
      this.projectid = value;
      fieldSetFlags()[9] = true;
      return this;
    }

    /**
      * Checks whether the 'projectid' field has been set.
      * @return True if the 'projectid' field has been set, false otherwise.
      */
    public boolean hasProjectid() {
      return fieldSetFlags()[9];
    }


    /**
      * Clears the value of the 'projectid' field.
      * @return This builder.
      */
    public com.madhouse.dsp.avro.ImpressionTrack.Builder clearProjectid() {
      fieldSetFlags()[9] = false;
      return this;
    }

    /**
      * Gets the value of the 'cid' field.
      * @return The value.
      */
    public java.lang.Long getCid() {
      return cid;
    }

    /**
      * Sets the value of the 'cid' field.
      * @param value The value of 'cid'.
      * @return This builder.
      */
    public com.madhouse.dsp.avro.ImpressionTrack.Builder setCid(long value) {
      validate(fields()[10], value);
      this.cid = value;
      fieldSetFlags()[10] = true;
      return this;
    }

    /**
      * Checks whether the 'cid' field has been set.
      * @return True if the 'cid' field has been set, false otherwise.
      */
    public boolean hasCid() {
      return fieldSetFlags()[10];
    }


    /**
      * Clears the value of the 'cid' field.
      * @return This builder.
      */
    public com.madhouse.dsp.avro.ImpressionTrack.Builder clearCid() {
      fieldSetFlags()[10] = false;
      return this;
    }

    /**
      * Gets the value of the 'crid' field.
      * @return The value.
      */
    public java.lang.Long getCrid() {
      return crid;
    }

    /**
      * Sets the value of the 'crid' field.
      * @param value The value of 'crid'.
      * @return This builder.
      */
    public com.madhouse.dsp.avro.ImpressionTrack.Builder setCrid(long value) {
      validate(fields()[11], value);
      this.crid = value;
      fieldSetFlags()[11] = true;
      return this;
    }

    /**
      * Checks whether the 'crid' field has been set.
      * @return True if the 'crid' field has been set, false otherwise.
      */
    public boolean hasCrid() {
      return fieldSetFlags()[11];
    }


    /**
      * Clears the value of the 'crid' field.
      * @return This builder.
      */
    public com.madhouse.dsp.avro.ImpressionTrack.Builder clearCrid() {
      fieldSetFlags()[11] = false;
      return this;
    }

    /**
      * Gets the value of the 'invalid' field.
      * @return The value.
      */
    public java.lang.Integer getInvalid() {
      return invalid;
    }

    /**
      * Sets the value of the 'invalid' field.
      * @param value The value of 'invalid'.
      * @return This builder.
      */
    public com.madhouse.dsp.avro.ImpressionTrack.Builder setInvalid(int value) {
      validate(fields()[12], value);
      this.invalid = value;
      fieldSetFlags()[12] = true;
      return this;
    }

    /**
      * Checks whether the 'invalid' field has been set.
      * @return True if the 'invalid' field has been set, false otherwise.
      */
    public boolean hasInvalid() {
      return fieldSetFlags()[12];
    }


    /**
      * Clears the value of the 'invalid' field.
      * @return This builder.
      */
    public com.madhouse.dsp.avro.ImpressionTrack.Builder clearInvalid() {
      fieldSetFlags()[12] = false;
      return this;
    }

    /**
      * Gets the value of the 'income' field.
      * @return The value.
      */
    public java.lang.Integer getIncome() {
      return income;
    }

    /**
      * Sets the value of the 'income' field.
      * @param value The value of 'income'.
      * @return This builder.
      */
    public com.madhouse.dsp.avro.ImpressionTrack.Builder setIncome(int value) {
      validate(fields()[13], value);
      this.income = value;
      fieldSetFlags()[13] = true;
      return this;
    }

    /**
      * Checks whether the 'income' field has been set.
      * @return True if the 'income' field has been set, false otherwise.
      */
    public boolean hasIncome() {
      return fieldSetFlags()[13];
    }


    /**
      * Clears the value of the 'income' field.
      * @return This builder.
      */
    public com.madhouse.dsp.avro.ImpressionTrack.Builder clearIncome() {
      fieldSetFlags()[13] = false;
      return this;
    }

    /**
      * Gets the value of the 'cost' field.
      * @return The value.
      */
    public java.lang.Integer getCost() {
      return cost;
    }

    /**
      * Sets the value of the 'cost' field.
      * @param value The value of 'cost'.
      * @return This builder.
      */
    public com.madhouse.dsp.avro.ImpressionTrack.Builder setCost(int value) {
      validate(fields()[14], value);
      this.cost = value;
      fieldSetFlags()[14] = true;
      return this;
    }

    /**
      * Checks whether the 'cost' field has been set.
      * @return True if the 'cost' field has been set, false otherwise.
      */
    public boolean hasCost() {
      return fieldSetFlags()[14];
    }


    /**
      * Clears the value of the 'cost' field.
      * @return This builder.
      */
    public com.madhouse.dsp.avro.ImpressionTrack.Builder clearCost() {
      fieldSetFlags()[14] = false;
      return this;
    }

    /**
      * Gets the value of the 'location' field.
      * @return The value.
      */
    public java.lang.CharSequence getLocation() {
      return location;
    }

    /**
      * Sets the value of the 'location' field.
      * @param value The value of 'location'.
      * @return This builder.
      */
    public com.madhouse.dsp.avro.ImpressionTrack.Builder setLocation(java.lang.CharSequence value) {
      validate(fields()[15], value);
      this.location = value;
      fieldSetFlags()[15] = true;
      return this;
    }

    /**
      * Checks whether the 'location' field has been set.
      * @return True if the 'location' field has been set, false otherwise.
      */
    public boolean hasLocation() {
      return fieldSetFlags()[15];
    }


    /**
      * Clears the value of the 'location' field.
      * @return This builder.
      */
    public com.madhouse.dsp.avro.ImpressionTrack.Builder clearLocation() {
      location = null;
      fieldSetFlags()[15] = false;
      return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public ImpressionTrack build() {
      try {
        ImpressionTrack record = new ImpressionTrack();
        record.time = fieldSetFlags()[0] ? this.time : (java.lang.Long) defaultValue(fields()[0]);
        record.ua = fieldSetFlags()[1] ? this.ua : (java.lang.CharSequence) defaultValue(fields()[1]);
        record.ip = fieldSetFlags()[2] ? this.ip : (java.lang.CharSequence) defaultValue(fields()[2]);
        record.args = fieldSetFlags()[3] ? this.args : (java.lang.CharSequence) defaultValue(fields()[3]);
        record.status = fieldSetFlags()[4] ? this.status : (java.lang.Integer) defaultValue(fields()[4]);
        record.bid = fieldSetFlags()[5] ? this.bid : (java.lang.CharSequence) defaultValue(fields()[5]);
        record.bidid = fieldSetFlags()[6] ? this.bidid : (java.lang.CharSequence) defaultValue(fields()[6]);
        record.mediaid = fieldSetFlags()[7] ? this.mediaid : (java.lang.Long) defaultValue(fields()[7]);
        record.adspaceid = fieldSetFlags()[8] ? this.adspaceid : (java.lang.Long) defaultValue(fields()[8]);
        record.projectid = fieldSetFlags()[9] ? this.projectid : (java.lang.Long) defaultValue(fields()[9]);
        record.cid = fieldSetFlags()[10] ? this.cid : (java.lang.Long) defaultValue(fields()[10]);
        record.crid = fieldSetFlags()[11] ? this.crid : (java.lang.Long) defaultValue(fields()[11]);
        record.invalid = fieldSetFlags()[12] ? this.invalid : (java.lang.Integer) defaultValue(fields()[12]);
        record.income = fieldSetFlags()[13] ? this.income : (java.lang.Integer) defaultValue(fields()[13]);
        record.cost = fieldSetFlags()[14] ? this.cost : (java.lang.Integer) defaultValue(fields()[14]);
        record.location = fieldSetFlags()[15] ? this.location : (java.lang.CharSequence) defaultValue(fields()[15]);
        return record;
      } catch (java.lang.Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumWriter<ImpressionTrack>
    WRITER$ = (org.apache.avro.io.DatumWriter<ImpressionTrack>)MODEL$.createDatumWriter(SCHEMA$);

  @Override public void writeExternal(java.io.ObjectOutput out)
    throws java.io.IOException {
    WRITER$.write(this, SpecificData.getEncoder(out));
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumReader<ImpressionTrack>
    READER$ = (org.apache.avro.io.DatumReader<ImpressionTrack>)MODEL$.createDatumReader(SCHEMA$);

  @Override public void readExternal(java.io.ObjectInput in)
    throws java.io.IOException {
    READER$.read(this, SpecificData.getDecoder(in));
  }

}