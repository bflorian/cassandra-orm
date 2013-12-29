package com.reachlocal.grails.plugins.cassandra.mapping

import com.reachlocal.grails.plugins.cassandra.utils.NestedHashMap

import java.text.DecimalFormat


class Profiler
{
	private map = new NestedHashMap()
	private ids = new LinkedHashSet()

	synchronized initialIncrement(String name, Long value, Object id) {
		ids.add(id)
		map.increment("initialize", 1L)
		map.increment(name, value)
	}

	synchronized increment(String name, Long value) {
		map.increment(name, value)
	}

	synchronized finalIncrement(String name, Long value, Object id) {
		map.increment(name, value)
		map.increment("finalize", 1L)
		ids.remove(id)
	}

	synchronized void clear() {
		map = new NestedHashMap()
	}

	synchronized Map data() {
		Map result = [:]
		result.putAll(map)
		result
	}

	synchronized Map averages() {
		Map result = [Iterations: map.Iterations, Exits: map.Exits, initialize: map.initialize, finalize: map.finalize]
		def iter = map.Iterations
		def sum = 0
		map.each {k,v ->
			if (!(["Iterations", "Exits", "initialize", "finalize"].contains(k))) {
				result[k] = v / (1000000L * iter)
				sum += v
			}
		}
		result.total = iter ? sum / (1000000L * iter) : null
		result
	}


	def summary() {
		def total = 0L
		def m = 1000000L
		def nf = new DecimalFormat("#,##0.0")
		println "PROFILER, ${profiler.Iterations} ITERATIONS (msec):"
		data().each {name, value ->
			if (!(["Iterations", "Exits", "initialize", "finalize"].contains(name))) {
				println "$name: \t${nf.format(value/m)}"
				total += value
			}
		}
		println "TOTAL: \t${nf.format(total/m)} \t${total/(profiler.Iterations*m)} \t${nf.format(1000L * m * profiler.Iterations/total)}\trec/sec"
	}

	def remainingIds() {
		ids as List
	}
}
