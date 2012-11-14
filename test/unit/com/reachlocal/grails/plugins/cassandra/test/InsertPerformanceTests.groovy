package com.reachlocal.grails.plugins.cassandra.test

import org.junit.Test;
import static org.junit.Assert.*
import com.reachlocal.grails.plugins.cassandra.test.orm.Visit

class InsertPerformanceTests extends OrmTestCase
{
    @Test
    void testAll()
    {
        initialize()

        for (k in 1..5) {
            def num = 100
            def t0 = System.currentTimeMillis()
            for (i in 1..num) {
                def v = new Visit(
                        siteName: "SITE_01",
                        occurTime: new Date(),
                        referrerType: "Search",
                        referrerName: "Google",
                        referrerKeyword: "Super duper",
                        referrerUrl: "http://www.reachlocal.com",
                        pageUrl: "http://docs.mongodb.org/manual/faq/developers/",
                        userAgent: "Chrome"
                )
                v.save(nocheck: true)
            }
            def elapsed = System.currentTimeMillis() - t0
            println "Inserted $num records in $elapsed msec, ${num / (elapsed / 1000.0)} rec/sec, ${elapsed / num} msec/rec"
        }
    }
}
