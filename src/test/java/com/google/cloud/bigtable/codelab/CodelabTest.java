package com.google.cloud.bigtable.codelab;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple Codelab.
 */
public class CodelabTest
    extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public CodelabTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( CodelabTest.class );
    }

    /**
     * Rigourous Test :-)
     */
    public void testCodelab()
    {
        assertTrue( true );
    }
}
