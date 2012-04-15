package com.reachlocal.grails.plugins.cassandra.utils

/**
 * @author: Bob Florian
 */
class HashCounter extends LinkedHashMap
{
	void set(name, value)
	{
		put(name, value)
	}

	void increment(name, value=1)
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

	List entries() {
		return map.collect{it}
	}

	Map map() {
		return this
	}
}
