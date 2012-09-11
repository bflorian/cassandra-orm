package com.reachlocal.grails.plugins.cassandra.mapping

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
			lastClass = BaseUtils.safeGetStaticProperty(lastClass, 'hasMany')?.get(node.name)
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
		if (opts.max) {
			keys = keys[0..opts.max-1]
		}

		def result = []
		def options = BaseUtils.addOptionDefaults(opts, BaseUtils.MAX_ROWS)
		def cassandra = traverser.object.cassandra
		def persistence = cassandra.persistence
		def names = MappingUtils.columnNames(options)
		def itemColumnFamily = segments[-1].itemClass.columnFamily
		def listClass = LinkedList // TODO - calculate?
		cassandra.withKeyspace(traverser.object.keySpace, traverser.object.cassandraCluster) {ks ->
			if (names) {
				def rows = persistence.getRowsColumnSlice(ks, itemColumnFamily, keys, names, options.consistencyLevel)
				result = cassandra.mapping.makeResult(keys, rows, options, listClass)
			}
			else {
				def rows = persistence.getRows(ks, itemColumnFamily, keys, options.consistencyLevel)
				result = cassandra.mapping.makeResult(keys, rows, options, listClass)
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
