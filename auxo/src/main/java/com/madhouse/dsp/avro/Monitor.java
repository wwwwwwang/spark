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
public class Monitor extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  private static final long serialVersionUID = 382778066738783112L;
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Monitor\",\"namespace\":\"com.madhouse.dsp.avro\",\"fields\":[{\"name\":\"impurl\",\"type\":[\"null\",{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"Track\",\"fields\":[{\"name\":\"startdelay\",\"type\":\"int\",\"default\":0},{\"name\":\"url\",\"type\":\"string\"}]}}],\"default\":null},{\"name\":\"clkurl\",\"type\":[\"null\",{\"type\":\"array\",\"items\":\"string\"}],\"default\":null},{\"name\":\"securl\",\"type\":[\"null\",{\"type\":\"array\",\"items\":\"string\"}],\"default\":null},{\"name\":\"exptime\",\"type\":\"int\",\"default\":86400},{\"name\":\"exts\",\"type\":[\"null\",{\"type\":\"array\",\"items\":\"string\"}],\"default\":null}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }

  private static SpecificData MODEL$ = new SpecificData();

  private static final BinaryMessageEncoder<Monitor> ENCODER =
      new BinaryMessageEncoder<Monitor>(MODEL$, SCHEMA$);

  private static final BinaryMessageDecoder<Monitor> DECODER =
      new BinaryMessageDecoder<Monitor>(MODEL$, SCHEMA$);

  /**
   * Return the BinaryMessageDecoder instance used by this class.
   */
  public static BinaryMessageDecoder<Monitor> getDecoder() {
    return DECODER;
  }

  /**
   * Create a new BinaryMessageDecoder instance for this class that uses the specified {@link SchemaStore}.
   * @param resolver a {@link SchemaStore} used to find schemas by fingerprint
   */
  public static BinaryMessageDecoder<Monitor> createDecoder(SchemaStore resolver) {
    return new BinaryMessageDecoder<Monitor>(MODEL$, SCHEMA$, resolver);
  }

  /** Serializes this Monitor to a ByteBuffer. */
  public java.nio.ByteBuffer toByteBuffer() throws java.io.IOException {
    return ENCODER.encode(this);
  }

  /** Deserializes a Monitor from a ByteBuffer. */
  public static Monitor fromByteBuffer(
      java.nio.ByteBuffer b) throws java.io.IOException {
    return DECODER.decode(b);
  }

  @Deprecated public java.util.List<com.madhouse.dsp.avro.Track> impurl;
  @Deprecated public java.util.List<java.lang.CharSequence> clkurl;
  @Deprecated public java.util.List<java.lang.CharSequence> securl;
  @Deprecated public int exptime;
  @Deprecated public java.util.List<java.lang.CharSequence> exts;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>.
   */
  public Monitor() {}

  /**
   * All-args constructor.
   * @param impurl The new value for impurl
   * @param clkurl The new value for clkurl
   * @param securl The new value for securl
   * @param exptime The new value for exptime
   * @param exts The new value for exts
   */
  public Monitor(java.util.List<com.madhouse.dsp.avro.Track> impurl, java.util.List<java.lang.CharSequence> clkurl, java.util.List<java.lang.CharSequence> securl, java.lang.Integer exptime, java.util.List<java.lang.CharSequence> exts) {
    this.impurl = impurl;
    this.clkurl = clkurl;
    this.securl = securl;
    this.exptime = exptime;
    this.exts = exts;
  }

  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call.
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return impurl;
    case 1: return clkurl;
    case 2: return securl;
    case 3: return exptime;
    case 4: return exts;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  // Used by DatumReader.  Applications should not call.
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: impurl = (java.util.List<com.madhouse.dsp.avro.Track>)value$; break;
    case 1: clkurl = (java.util.List<java.lang.CharSequence>)value$; break;
    case 2: securl = (java.util.List<java.lang.CharSequence>)value$; break;
    case 3: exptime = (java.lang.Integer)value$; break;
    case 4: exts = (java.util.List<java.lang.CharSequence>)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'impurl' field.
   * @return The value of the 'impurl' field.
   */
  public java.util.List<com.madhouse.dsp.avro.Track> getImpurl() {
    return impurl;
  }

  /**
   * Sets the value of the 'impurl' field.
   * @param value the value to set.
   */
  public void setImpurl(java.util.List<com.madhouse.dsp.avro.Track> value) {
    this.impurl = value;
  }

  /**
   * Gets the value of the 'clkurl' field.
   * @return The value of the 'clkurl' field.
   */
  public java.util.List<java.lang.CharSequence> getClkurl() {
    return clkurl;
  }

  /**
   * Sets the value of the 'clkurl' field.
   * @param value the value to set.
   */
  public void setClkurl(java.util.List<java.lang.CharSequence> value) {
    this.clkurl = value;
  }

  /**
   * Gets the value of the 'securl' field.
   * @return The value of the 'securl' field.
   */
  public java.util.List<java.lang.CharSequence> getSecurl() {
    return securl;
  }

  /**
   * Sets the value of the 'securl' field.
   * @param value the value to set.
   */
  public void setSecurl(java.util.List<java.lang.CharSequence> value) {
    this.securl = value;
  }

  /**
   * Gets the value of the 'exptime' field.
   * @return The value of the 'exptime' field.
   */
  public java.lang.Integer getExptime() {
    return exptime;
  }

  /**
   * Sets the value of the 'exptime' field.
   * @param value the value to set.
   */
  public void setExptime(java.lang.Integer value) {
    this.exptime = value;
  }

  /**
   * Gets the value of the 'exts' field.
   * @return The value of the 'exts' field.
   */
  public java.util.List<java.lang.CharSequence> getExts() {
    return exts;
  }

  /**
   * Sets the value of the 'exts' field.
   * @param value the value to set.
   */
  public void setExts(java.util.List<java.lang.CharSequence> value) {
    this.exts = value;
  }

  /**
   * Creates a new Monitor RecordBuilder.
   * @return A new Monitor RecordBuilder
   */
  public static com.madhouse.dsp.avro.Monitor.Builder newBuilder() {
    return new com.madhouse.dsp.avro.Monitor.Builder();
  }

  /**
   * Creates a new Monitor RecordBuilder by copying an existing Builder.
   * @param other The existing builder to copy.
   * @return A new Monitor RecordBuilder
   */
  public static com.madhouse.dsp.avro.Monitor.Builder newBuilder(com.madhouse.dsp.avro.Monitor.Builder other) {
    return new com.madhouse.dsp.avro.Monitor.Builder(other);
  }

  /**
   * Creates a new Monitor RecordBuilder by copying an existing Monitor instance.
   * @param other The existing instance to copy.
   * @return A new Monitor RecordBuilder
   */
  public static com.madhouse.dsp.avro.Monitor.Builder newBuilder(com.madhouse.dsp.avro.Monitor other) {
    return new com.madhouse.dsp.avro.Monitor.Builder(other);
  }

  /**
   * RecordBuilder for Monitor instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<Monitor>
    implements org.apache.avro.data.RecordBuilder<Monitor> {

    private java.util.List<com.madhouse.dsp.avro.Track> impurl;
    private java.util.List<java.lang.CharSequence> clkurl;
    private java.util.List<java.lang.CharSequence> securl;
    private int exptime;
    private java.util.List<java.lang.CharSequence> exts;

    /** Creates a new Builder */
    private Builder() {
      super(SCHEMA$);
    }

    /**
     * Creates a Builder by copying an existing Builder.
     * @param other The existing Builder to copy.
     */
    private Builder(com.madhouse.dsp.avro.Monitor.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.impurl)) {
        this.impurl = data().deepCopy(fields()[0].schema(), other.impurl);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.clkurl)) {
        this.clkurl = data().deepCopy(fields()[1].schema(), other.clkurl);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.securl)) {
        this.securl = data().deepCopy(fields()[2].schema(), other.securl);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.exptime)) {
        this.exptime = data().deepCopy(fields()[3].schema(), other.exptime);
        fieldSetFlags()[3] = true;
      }
      if (isValidValue(fields()[4], other.exts)) {
        this.exts = data().deepCopy(fields()[4].schema(), other.exts);
        fieldSetFlags()[4] = true;
      }
    }

    /**
     * Creates a Builder by copying an existing Monitor instance
     * @param other The existing instance to copy.
     */
    private Builder(com.madhouse.dsp.avro.Monitor other) {
            super(SCHEMA$);
      if (isValidValue(fields()[0], other.impurl)) {
        this.impurl = data().deepCopy(fields()[0].schema(), other.impurl);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.clkurl)) {
        this.clkurl = data().deepCopy(fields()[1].schema(), other.clkurl);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.securl)) {
        this.securl = data().deepCopy(fields()[2].schema(), other.securl);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.exptime)) {
        this.exptime = data().deepCopy(fields()[3].schema(), other.exptime);
        fieldSetFlags()[3] = true;
      }
      if (isValidValue(fields()[4], other.exts)) {
        this.exts = data().deepCopy(fields()[4].schema(), other.exts);
        fieldSetFlags()[4] = true;
      }
    }

    /**
      * Gets the value of the 'impurl' field.
      * @return The value.
      */
    public java.util.List<com.madhouse.dsp.avro.Track> getImpurl() {
      return impurl;
    }

    /**
      * Sets the value of the 'impurl' field.
      * @param value The value of 'impurl'.
      * @return This builder.
      */
    public com.madhouse.dsp.avro.Monitor.Builder setImpurl(java.util.List<com.madhouse.dsp.avro.Track> value) {
      validate(fields()[0], value);
      this.impurl = value;
      fieldSetFlags()[0] = true;
      return this;
    }

    /**
      * Checks whether the 'impurl' field has been set.
      * @return True if the 'impurl' field has been set, false otherwise.
      */
    public boolean hasImpurl() {
      return fieldSetFlags()[0];
    }


    /**
      * Clears the value of the 'impurl' field.
      * @return This builder.
      */
    public com.madhouse.dsp.avro.Monitor.Builder clearImpurl() {
      impurl = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    /**
      * Gets the value of the 'clkurl' field.
      * @return The value.
      */
    public java.util.List<java.lang.CharSequence> getClkurl() {
      return clkurl;
    }

    /**
      * Sets the value of the 'clkurl' field.
      * @param value The value of 'clkurl'.
      * @return This builder.
      */
    public com.madhouse.dsp.avro.Monitor.Builder setClkurl(java.util.List<java.lang.CharSequence> value) {
      validate(fields()[1], value);
      this.clkurl = value;
      fieldSetFlags()[1] = true;
      return this;
    }

    /**
      * Checks whether the 'clkurl' field has been set.
      * @return True if the 'clkurl' field has been set, false otherwise.
      */
    public boolean hasClkurl() {
      return fieldSetFlags()[1];
    }


    /**
      * Clears the value of the 'clkurl' field.
      * @return This builder.
      */
    public com.madhouse.dsp.avro.Monitor.Builder clearClkurl() {
      clkurl = null;
      fieldSetFlags()[1] = false;
      return this;
    }

    /**
      * Gets the value of the 'securl' field.
      * @return The value.
      */
    public java.util.List<java.lang.CharSequence> getSecurl() {
      return securl;
    }

    /**
      * Sets the value of the 'securl' field.
      * @param value The value of 'securl'.
      * @return This builder.
      */
    public com.madhouse.dsp.avro.Monitor.Builder setSecurl(java.util.List<java.lang.CharSequence> value) {
      validate(fields()[2], value);
      this.securl = value;
      fieldSetFlags()[2] = true;
      return this;
    }

    /**
      * Checks whether the 'securl' field has been set.
      * @return True if the 'securl' field has been set, false otherwise.
      */
    public boolean hasSecurl() {
      return fieldSetFlags()[2];
    }


    /**
      * Clears the value of the 'securl' field.
      * @return This builder.
      */
    public com.madhouse.dsp.avro.Monitor.Builder clearSecurl() {
      securl = null;
      fieldSetFlags()[2] = false;
      return this;
    }

    /**
      * Gets the value of the 'exptime' field.
      * @return The value.
      */
    public java.lang.Integer getExptime() {
      return exptime;
    }

    /**
      * Sets the value of the 'exptime' field.
      * @param value The value of 'exptime'.
      * @return This builder.
      */
    public com.madhouse.dsp.avro.Monitor.Builder setExptime(int value) {
      validate(fields()[3], value);
      this.exptime = value;
      fieldSetFlags()[3] = true;
      return this;
    }

    /**
      * Checks whether the 'exptime' field has been set.
      * @return True if the 'exptime' field has been set, false otherwise.
      */
    public boolean hasExptime() {
      return fieldSetFlags()[3];
    }


    /**
      * Clears the value of the 'exptime' field.
      * @return This builder.
      */
    public com.madhouse.dsp.avro.Monitor.Builder clearExptime() {
      fieldSetFlags()[3] = false;
      return this;
    }

    /**
      * Gets the value of the 'exts' field.
      * @return The value.
      */
    public java.util.List<java.lang.CharSequence> getExts() {
      return exts;
    }

    /**
      * Sets the value of the 'exts' field.
      * @param value The value of 'exts'.
      * @return This builder.
      */
    public com.madhouse.dsp.avro.Monitor.Builder setExts(java.util.List<java.lang.CharSequence> value) {
      validate(fields()[4], value);
      this.exts = value;
      fieldSetFlags()[4] = true;
      return this;
    }

    /**
      * Checks whether the 'exts' field has been set.
      * @return True if the 'exts' field has been set, false otherwise.
      */
    public boolean hasExts() {
      return fieldSetFlags()[4];
    }


    /**
      * Clears the value of the 'exts' field.
      * @return This builder.
      */
    public com.madhouse.dsp.avro.Monitor.Builder clearExts() {
      exts = null;
      fieldSetFlags()[4] = false;
      return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Monitor build() {
      try {
        Monitor record = new Monitor();
        record.impurl = fieldSetFlags()[0] ? this.impurl : (java.util.List<com.madhouse.dsp.avro.Track>) defaultValue(fields()[0]);
        record.clkurl = fieldSetFlags()[1] ? this.clkurl : (java.util.List<java.lang.CharSequence>) defaultValue(fields()[1]);
        record.securl = fieldSetFlags()[2] ? this.securl : (java.util.List<java.lang.CharSequence>) defaultValue(fields()[2]);
        record.exptime = fieldSetFlags()[3] ? this.exptime : (java.lang.Integer) defaultValue(fields()[3]);
        record.exts = fieldSetFlags()[4] ? this.exts : (java.util.List<java.lang.CharSequence>) defaultValue(fields()[4]);
        return record;
      } catch (java.lang.Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumWriter<Monitor>
    WRITER$ = (org.apache.avro.io.DatumWriter<Monitor>)MODEL$.createDatumWriter(SCHEMA$);

  @Override public void writeExternal(java.io.ObjectOutput out)
    throws java.io.IOException {
    WRITER$.write(this, SpecificData.getEncoder(out));
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumReader<Monitor>
    READER$ = (org.apache.avro.io.DatumReader<Monitor>)MODEL$.createDatumReader(SCHEMA$);

  @Override public void readExternal(java.io.ObjectInput in)
    throws java.io.IOException {
    READER$.read(this, SpecificData.getDecoder(in));
  }

}
