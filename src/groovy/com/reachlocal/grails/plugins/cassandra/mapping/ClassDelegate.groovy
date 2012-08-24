package com.reachlocal.grails.plugins.cassandra.mapping

/**
 * @author: Bob Florian
 */
class ClassDelegate
{
	@Delegate(interfaces=false) Class clazz
	String cassandraCluster
	Object instance

	ClassDelegate(Class clazz, String cluster)
	{
		this.clazz = clazz
		this.cassandraCluster = cluster
		this.instance = clazz.newInstance() // cheating!
	}

	def getCassandraCluster()
	{
		return this.cassandraCluster
	}

	def get(id, opts=[:])
	{
		def result = instance.get(id, opts)
		if (result) {
			result._cassandra_cluster_ = cassandraCluster
		}
		return result
	}

	def methodMissing(String name, args)
	{
		//println "METHOD: $name ($args)"
		instance.invokeMethod(name, args)
	}

	def propertyMissing(String name)
	{
		//println "PROPERTY: $name"
		instance.getProperty(name)
	}
}
