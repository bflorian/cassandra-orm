package com.reachlocal.grails.plugins.cassandra.test.orm

/**
 * @author: Bob Florian
 */
class Visit
{
	UUID uuid
	String siteName
	String referrerType
	String referrerName
    String referrerUrl
    String referrerKeyword
    String pageUrl
    String userAgent
	Date occurTime

	static transients = ["ageInDays"]

	static cassandraMapping = [
			unindexedPrimaryKey: 'uuid',
			explicitIndexes: [
			        'siteName'
			]
	]

	Integer getAgeInDays()
	{
		return new Date() - occurTime
	}
}
