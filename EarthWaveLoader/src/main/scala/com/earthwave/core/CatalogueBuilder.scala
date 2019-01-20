package com.earthwave.core

import akka.actor.{Actor, ActorLogging}
import com.earthwave.core.Messages.Flush
import com.stream_financial.publib.connector.ConnectorSession
import com.stream_financial.publib.table.DFTable

class CatalogueBuilder extends Actor with ActorLogging {

  var shards = List[ShardDetail]()

  def receive = {
    case  s : ShardDetail =>{
      shards = s :: shards
    }
    case Flush() =>{

      val session = new ConnectorSession("localhost",9001, "Earthwave//Data0")

      val catalogue = new DFTable("Catalogue" )

      catalogue.add( DFHelper.createColumnString("dsName", shards.map(x => x.dsName ).toVector) )
      catalogue.add( DFHelper.createColumnString("SwathFileName", shards.map( x => x.swathName).toVector))
      catalogue.add( DFHelper.createColumnString("CDBShardName", shards.map(x => x.shardName).toVector))
      catalogue.add( DFHelper.createColumnLong("GridCellMinX", shards.map(x => x.gridCellMinX ).toVector ) )
      catalogue.add( DFHelper.createColumnLong("GridCellMaxX", shards.map(x => x.gridCellMaxX ).toVector ) )
      catalogue.add( DFHelper.createColumnLong("GridCellMinY", shards.map(x => x.gridCellMinY ).toVector ) )
      catalogue.add( DFHelper.createColumnLong("GridCellMaxY", shards.map(x => x.gridCellMaxY ).toVector ) )
      catalogue.add( DFHelper.createColumnLong("GridCellSize", shards.map(s => s.gridCellSize).toVector ) )
      catalogue.add( DFHelper.createColumnLong("ShardMinX", shards.map(x => x.minX).toVector ) )
      catalogue.add( DFHelper.createColumnLong("ShardMaxX", shards.map(x => x.maxX).toVector ) )
      catalogue.add( DFHelper.createColumnLong("ShardMinY", shards.map(x => x.minY).toVector ) )
      catalogue.add( DFHelper.createColumnLong("ShardMaxY", shards.map(x => x.maxY).toVector ) )
      catalogue.add( DFHelper.createColumnLong("ShardMinTime", shards.map(x => x.minTime).toVector ) )
      catalogue.add( DFHelper.createColumnLong("ShardMaxTime", shards.map(x => x.maxTime).toVector ) )
      catalogue.add( DFHelper.createColumnLong("CountPointsInShard", shards.map(x => x.count).toVector))

      val status = session.uploadTable(catalogue)

      session.close()

      log.info(s"Catalogue written: ${status}")

      sender ! "Catalogue creation complete"

    }
  }
}
