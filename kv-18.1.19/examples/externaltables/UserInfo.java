/*-
 * Copyright (C) 2011, 2018 Oracle and/or its affiliates. All rights reserved.
 *
 * This file was distributed by Oracle as part of a version of Oracle NoSQL
 * Database made available at:
 *
 * http://www.oracle.com/technetwork/database/database-technologies/nosqldb/downloads/index.html
 *
 * Please see the LICENSE file included in the top-level directory of the
 * appropriate version of Oracle NoSQL Database for a copy of the license and
 * additional information.
 */

package externaltables;

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;

import oracle.kv.Key;
import oracle.kv.Value;

/**
 * A simple class which represents a User in the External Tables Cookbook
 * example.
 */
public class UserInfo {

    /*
     * The email address is a unique identifier and is used to construct
     * the Key's major path.
     */
    private final String email;

    /* Persistent fields stored in the Value. */
    private String name;
    private String gender;
    private String address;
    private String phone;

    private static final String INFO_PROPERTY_NAME = "info";

    /**
     * Constructs a user object with its unique identifier, the email address.
     */
    public UserInfo(final String email) {
        this.email = email;
    }

    /**
     * Returns the email identifier.
     */
    public String getEmail() {
        return email;
    }

    /**
     * Changes the name attribute.
     */
    public void setName(final String name) {
        this.name = name;
    }

    /**
     * Returns the name attribute.
     */
    public String getName() {
        return name;
    }

    /**
     * Changes the gender attribute.
     */
    public void setGender(final String gender) {
        this.gender = gender;
    }

    /**
     * Returns the gender attribute.
     */
    public String getGender() {
        return gender;
    }

    /**
     * Changes the address attribute.
     */
    public void setAddress(final String address) {
        this.address = address;
    }

    /**
     * Returns the address attribute.
     */
    public String getAddress() {
        return address;
    }

    /**
     * Changes the phone attribute.
     */
    public void setPhone(final String phone) {
        this.phone = phone;
    }

    /**
     * Returns the phone attribute.
     */
    public String getPhone() {
        return phone;
    }

    /**
     * Returns a Key that can be used to write or read the UserInfo.
     */
    public Key getStoreKey() {
        return Key.createKey(Arrays.asList(LoadCookbookData.USER_OBJECT_TYPE,
                                           email),
                             INFO_PROPERTY_NAME);
    }

    /**
     * Serializes user info attributes into the byte array of a Value.
     */
    public Value getStoreValue() {

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final DataOutputStream dos = new DataOutputStream(baos);

        try {
            writeString(dos, name);
            writeString(dos, gender);
            writeString(dos, address);
            writeString(dos, phone);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return Value.createValue(baos.toByteArray());
    }

    /**
     * Utility that writes a UTF string and accomodates null values.
     */
    private void writeString(final DataOutput out, final String val)
        throws IOException {

        if (val == null) {
            out.writeBoolean(false);
            return;
        }
        out.writeBoolean(true);
        out.writeUTF(val);
    }

    @Override
    public String toString() {
        return "<UserInfo " + email +
               "\n    name: " + name + ", gender: " + gender + "," +
               "\n    address: " + address + ", phone: " + phone +
               ">";
    }
}
