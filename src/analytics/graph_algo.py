import logging
from neo4j import GraphDatabase

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

NEO4J_URI = "neo4j://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "citycycle"


class GraphAnalytics:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
    
    def close(self):
        self.driver.close()
    
    def pagerank(self, max_iterations=20):
        """
        Run PageRank algorithm on the station graph to identify hub stations.
        Returns stations sorted by importance (PageRank score).
        """
        graph_name = "citycycle_graph"
        
        with self.driver.session() as session:
            # Project the graph into memory
            try:
                session.run("CALL gds.graph.drop($graph_name)", graph_name=graph_name)
            except:
                pass  # Graph might not exist yet
            
            session.run("""
                CALL gds.graph.project(
                    $graph_name,
                    'Station',
                    'RIDE'
                )
            """, graph_name=graph_name)
            
            # Run PageRank
            result = session.run("""
                CALL gds.pageRank.stream($graph_name, {
                    maxIterations: $max_iter
                })
                YIELD nodeId, score
                RETURN gds.util.asNode(nodeId).name AS station_name, 
                       gds.util.asNode(nodeId).id AS station_id,
                       score
                ORDER BY score DESC
                LIMIT 10
            """, graph_name=graph_name, max_iter=max_iterations)
            
            stations = [dict(record) for record in result]
            
            # Drop the graph projection
            session.run("CALL gds.graph.drop($graph_name)", graph_name=graph_name)
            
            return stations
    
    def get_stats(self):
        """Get basic graph statistics"""
        with self.driver.session() as session:
            result = session.run("""
                MATCH (s:Station)
                WITH count(s) AS station_count
                MATCH ()-[r:RIDE]->()
                RETURN station_count, count(r) AS ride_count
            """)
            
            return dict(result.single())


if __name__ == "__main__":
    analytics = GraphAnalytics(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
    
    # Get stats
    stats = analytics.get_stats()
    logger.info(f"Graph Stats: {stats}")
    
    # Run PageRank
    logger.info("Running PageRank to find hub stations...")
    hubs = analytics.pagerank()
    
    logger.info("Top 10 Hub Stations:")
    for i, station in enumerate(hubs, 1):
        logger.info(f"{i}. {station['station_name']} (Score: {station['score']:.4f})")
    
    analytics.close()
