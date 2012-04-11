package com.reachlocal.grails.plugins.cassandra.utils

/**
 * @author: Bob Florian
 */
class HashCounter extends LinkedHashMap
{
	synchronized void set(name, value)
	{
		put(name, value)
	}

	synchronized void increment(name, value=1)
	{
		def entry = get(name)
		if (entry) {
			put(name, entry + value)
		}
		else {
			put(name, value)
		}
	}

	def value(name)
	{
		get(name)
	}

	synchronized List entries() {
		return map.collect{it}
	}

	synchronized Map map() {
		return this
	}
}
