package com.reachlocal.grails.plugins.cassandra.mapping

import org.apache.commons.beanutils.PropertyUtils
import com.reachlocal.grails.plugins.cassandra.utils.OrmHelper

/**
 * @author: Bob Florian
 */
class TraverserNode 
{
	Traverser traverser
	TraverserNode parent
	String name
	def args
	def clazz
	def itemClass

	def propertyMissing(String name)
	{
		return new TraverserNode(traverser: traverser, parent: this, name: name)
	}

	def methodMissing(String name, args)
	{
		return new TraverserNode(traverser : traverser, parent: this, name: name, args: args)
	}

	def path()
	{
		parent ? parent.path() + [this] : [this]
	}

	def evaluate(objects)
	{
		//println "EVALUATE: ${objects}"
		def result = []
		if (args) {
			objects.each {obj ->
				//println "METHOD $name ($args)"
				result += obj.invokeMethod(name, args)
			}
		}
		else {
			objects.each {obj ->
				//println "PROPERTY: $name, value='${it.getProperty(name)}', object=${it.getClass().name}"
				result += obj.getProperty(name)
			}
		}
		return result
	}

	def evaluateKeys(keys, thisClass, propName, itemClass, opts)
	{
		def result = []
		keys.each {key ->
			result += MappingUtils.getKeysForMappedObject(thisClass, key, propName, itemClass, opts)
		}
		return result
	}

	def execute(opts=[:])
	{
		def segments = path()
		def lastClass = traverser.object.getClass()
		segments.collect {node ->
			node.clazz = lastClass
			lastClass = MappingUtils.safeGetStaticProperty(lastClass, 'hasMany')?.get(node.name)
			node.itemClass = lastClass
			if (!lastClass) {
				throw new InvalidObjectException("Could not find hasMany property ${node.name} in class ${lastClass.name}")
			}
		}

		def keys = [traverser.object.id]
		segments.each {n ->
			keys = evaluateKeys(keys, n.clazz, n.name, n.itemClass, n.args ? n.args[0] : [:])
		}

		if (opts.sort == null || opts.sort) {
			keys = keys.sort{it}
		}
		if (opts.reversed) {
			keys = keys.reverse()
		}
		if (opts.max && opts.max < keys.size()-1) {
			keys = keys[0..opts.max-1]
		}

		def result = []
		def options = OrmHelper.addOptionDefaults(opts, OrmHelper.MAX_ROWS)
		def cassandra = traverser.object.cassandra
		def persistence = cassandra.persistence
		def names = MappingUtils.columnNames(options)
		def iClass = segments[-1].itemClass
		def itemColumnFamily = iClass.columnFamily
		def oClass = iClass && List.isAssignableFrom(iClass) ? LinkedList : LinkedHashSet

		cassandra.withKeyspace(traverser.object.keySpace, traverser.object.cassandraCluster) {ks ->
			if (names) {
				def rows = persistence.getRowsColumnSlice(ks, itemColumnFamily, keys, names, options.consistencyLevel)
				result = cassandra.mapping.makeResult(keys, rows, options, oClass)
			}
			else {
				def rows = persistence.getRows(ks, itemColumnFamily, keys, options.consistencyLevel)
				result = cassandra.mapping.makeResult(keys, rows, options, oClass)
			}
		}
		return result
	}

	def executeOld(args=[:])
	{
		def segments = path()
		def items = [traverser.object]
		segments.each {n ->
			items = n.evaluate(items)
		}
		if (args.sort == null || args.sort) {
			items = items.sort{it.id}
		}
		if (args.reversed) {
			items = items.reverse()
		}
		if (args.max) {
			items = items[0..args.max-1]
		}
		return items
	}

	String toString()
	{
		"$name($args)"
	}
}
