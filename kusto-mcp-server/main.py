from mcp.server import FastMCP
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder, ClientRequestProperties
import uuid

# Initialize the FastMCP server
mcp = FastMCP()

# Global region URIs map
region_uris = {
    "eastus2": "https://sqlazureeus22.kustomfa.windows.net",
    "eastus1": "https://sqlazureeus12.kustomfa.windows.net",
    "centralus": "https://sqlazurecus2.kustomfa.windows.net",
    "australia": "https://sqlazureau2.kustomfa.windows.net",
    "brazil": "https://sqlazurebr2.kustomfa.windows.net",
    "canada": "https://sqlazureca2.kustomfa.windows.net",
    "france": "https://sqlazurefra.kustomfa.windows.net",
    "japan": "https://sqlazureja2.kustomfa.windows.net",
    "korea": "https://sqlazurekor.kustomfa.windows.net",
}

DATABASE_NAME = "sqlazure1"

@mcp.tool()
def get_engine_logs(region_name: str, server_name: str, start_time: str, end_time: str, search_key: str = None) -> str:
    """Get mysql server engine logs between start_time and end_time.
    
    Args:
        region_name: Name of the region where server is deployed
        server_name: Name of the server
        start_time: Start time of the query
        end_time: End time of the query
        search_key: Optional search key to filter logs
    """
    cluster_uri = region_uris.get(region_name)
    if not cluster_uri:
        return f"Region {region_name} is not supported"
    
    kcsb = KustoConnectionStringBuilder.with_az_cli_authentication(cluster_uri)

    result = []

    with KustoClient(kcsb) as client:
        crp = ClientRequestProperties()
        crp.client_request_id = "kusto-mcp-server" + str(uuid.uuid4())
        crp.set_parameter("target_server_name", server_name)
        crp.set_parameter("start_time", start_time)
        crp.set_parameter("end_time", end_time)
        crp.set_parameter("search_key", search_key)

        query = """
        declare query_parameters(target_server_name:string, start_time: string, end_time: string, search_key: string);
        let start = todatetime(start_time);
        let end = todatetime(end_time);
        MonMySQLLogs
        | where LogicalServerName == target_server_name
        | where TIMESTAMP between (start .. end)
        | where message contains search_key
        | project TIMESTAMP, message
        | limit 500
        """
        
        try:
            response = client.execute_query(DATABASE_NAME, query, crp)
            for row in response.primary_results[0]:
                result.append(f"Timestamp: {row['TIMESTAMP']}, Message: {row['message']}")
        except Exception as e:
            return f"Error executing query: {str(e)}"

        return "\n---\n".join(result)

@mcp.tool()
def get_launcher_logs(region_name: str, server_name: str, start_time: str, end_time: str, search_key: str = None) -> str:
    """Get messages for the launcher container of mysql server for the given time range.
    
    Args:
        region_name: Name of the region where server is deployed
        server_name: Name of the server
        start_time: Start time of the query
        end_time: End time of the query
        search_key: Optional search key to filter logs
    """
    cluster_uri = region_uris.get(region_name)
    if not cluster_uri:
        return f"Region {region_name} is not supported"
    
    kcsb = KustoConnectionStringBuilder.with_az_cli_authentication(cluster_uri)

    result = []

    with KustoClient(kcsb) as client:
        crp = ClientRequestProperties()
        crp.client_request_id = "kusto-mcp-server" + str(uuid.uuid4())
        crp.set_parameter("target_server_name", server_name)
        crp.set_parameter("start_time", start_time)
        crp.set_parameter("end_time", end_time)

        query = """
        declare query_parameters(target_server_name:string, start_time: string, end_time: string, search_key: string);
        let start = todatetime(start_time);
        let end = todatetime(end_time);
        MonMySQLLauncher
        | where LogicalServerName == target_server_name
        | where TIMESTAMP between(start .. end)
        | where message contains search_key
        | project TIMESTAMP, message
        | limit 500
        """
        
        try:
            response = client.execute_query(DATABASE_NAME, query, crp)
            for row in response.primary_results[0]:
                result.append(f"Timestamp: {row['TIMESTAMP']}, Message: {row['message']}")
        except Exception as e:
            return f"Error executing query: {str(e)}"

        return "\n---\n".join(result)

@mcp.tool()
def get_sidecar_logs(region_name: str, server_name: str, start_time: str, end_time: str, search_key: str = None) -> str:
    """Get messages for the sidecar container of mysql server for the given time range.
    
    Args:
        region_name: Name of the region where server is deployed
        server_name: Name of the server
        start_time: Start time of the query
        end_time: End time of the query
        search_key: Optional search key to filter logs
    """
    cluster_uri = region_uris.get(region_name)
    if not cluster_uri:
        return f"Region {region_name} is not supported"
    
    kcsb = KustoConnectionStringBuilder.with_az_cli_authentication(cluster_uri)

    result = []

    with KustoClient(kcsb) as client:
        crp = ClientRequestProperties()
        crp.client_request_id = "kusto-mcp-server" + str(uuid.uuid4())
        crp.set_parameter("target_server_name", server_name)
        crp.set_parameter("start_time", start_time)
        crp.set_parameter("end_time", end_time)
        crp.set_parameter("search_key", search_key)

        query = """
        declare query_parameters(target_server_name:string, start_time: string, end_time: string, search_key: string);
        let start = todatetime(start_time);
        let end = todatetime(end_time);
        MonMySQLSideCar
        | where LogicalServerName == target_server_name
        | where TIMESTAMP between(start .. end)
        | where message contains search_key
        | project TIMESTAMP, message
        | limit 500
        """
        
        try:
            response = client.execute_query(DATABASE_NAME, query, crp)
            for row in response.primary_results[0]:
                result.append(f"Timestamp: {row['TIMESTAMP']}, Message: {row['message']}")
        except Exception as e:
            return f"Error executing query: {str(e)}"

        return "\n---\n".join(result)
    
@mcp.tool()
def get_sidecar_logs_for_actor(region_name: str, server_name: str, actor_name:str, start_time: str, end_time: str, search_key: str = None) -> str:
    """Get messages for the sidecar container of mysql server for a particular actor between the given time range.
    
    Args:
        region_name: Name of the region where server is deployed
        server_name: Name of the server
        actor_name: Name of the actor
        start_time: Start time of the query
        end_time: End time of the query
        search_key: Optional search key to filter logs
    """
    cluster_uri = region_uris.get(region_name)
    if not cluster_uri:
        return f"Region {region_name} is not supported"
    
    kcsb = KustoConnectionStringBuilder.with_az_cli_authentication(cluster_uri)

    result = []

    with KustoClient(kcsb) as client:
        crp = ClientRequestProperties()
        crp.client_request_id = "kusto-mcp-server" + str(uuid.uuid4())
        crp.set_parameter("target_server_name", server_name)
        crp.set_parameter("actor_name", actor_name)
        crp.set_parameter("start_time", start_time)
        crp.set_parameter("end_time", end_time)
        crp.set_parameter("search_key", search_key)

        query = """
        declare query_parameters(target_server_name:string, actor_name:string, start_time: string, end_time: string, search_key: string);
        let start = todatetime(start_time);
        let end = todatetime(end_time);
        MonMySQLSideCar
        | where LogicalServerName == target_server_name
        | where SourceContext contains actor_name
        | where TIMESTAMP between(start .. end)
        | where message contains search_key
        | project TIMESTAMP, message
        """
        
        try:
            response = client.execute_query(DATABASE_NAME, query, crp)
            for row in response.primary_results[0]:
                result.append(f"Timestamp: {row['TIMESTAMP']}, Message: {row['message']}")
        except Exception as e:
            return f"Error executing query: {str(e)}"

        return "\n---\n".join(result)

@mcp.tool()
def get_director_logs(region_name: str, start_time: str, end_time: str, search_key: str = None) -> str:
    """Get messages for the director container or kubernetes(k8s) logs of mysql server between the given time range.

    Args:
        region_name: Name of the region where server is deployed
        start_time: Start time of the query
        end_time: End time of the query
        search_key: Optional search key to filter logs
    """
    cluster_uri = region_uris.get(region_name)
    if not cluster_uri:
        return f"Region {region_name} is not supported"
    
    kcsb = KustoConnectionStringBuilder.with_az_cli_authentication(cluster_uri)

    result = []

    with KustoClient(kcsb) as client:
        crp = ClientRequestProperties()
        crp.client_request_id = "kusto-mcp-server" + str(uuid.uuid4())
        crp.set_parameter("start_time", start_time)
        crp.set_parameter("end_time", end_time)
        crp.set_parameter("search_key", search_key)

        query = """
        declare query_parameters(start_time: string, end_time: string, search_key: string);
        let start = todatetime(start_time);
        let end = todatetime(end_time);
        MonMySQLDirector
        | project TIMESTAMP, SourceContext, message
        | where TIMESTAMP between(start .. end)
        | where message contains search_key
        | limit 500
        """
        
        try:
            response = client.execute_query(DATABASE_NAME, query, crp)
            for row in response.primary_results[0]:
                result.append(f"Timestamp: {row['TIMESTAMP']}, SourceContext: {row['SourceContext']}, Message: {row['message']}")
        except Exception as e:
            return f"Error executing query: {str(e)}"

        return "\n---\n".join(result)

@mcp.tool()
def get_director_logs_for_actor(region_name: str, reconciller_name:str, start_time: str, end_time: str, search_key: str = None) -> str:
    """Get messages for the director container or kubernetes(k8s) logs of mysql server for a particular reconciller between the given time range.
    
    Args:
        region_name: Name of the region where server is deployed
        reconciller_name: Name of the reconciller
        start_time: Start time of the query
        end_time: End time of the query
        search_key: Optional search key to filter logs
    """
    cluster_uri = region_uris.get(region_name)
    if not cluster_uri:
        return f"Region {region_name} is not supported"
    
    kcsb = KustoConnectionStringBuilder.with_az_cli_authentication(cluster_uri)

    result = []

    with KustoClient(kcsb) as client:
        crp = ClientRequestProperties()
        crp.client_request_id = "kusto-mcp-server" + str(uuid.uuid4())
        crp.set_parameter("reconciller_name", reconciller_name)
        crp.set_parameter("start_time", start_time)
        crp.set_parameter("end_time", end_time)
        crp.set_parameter("search_key", search_key)

        query = """
        declare query_parameters(reconciller_name:string, start_time: string, end_time: string, search_key: string);
        let start = todatetime(start_time);
        let end = todatetime(end_time);
        MonMySQLDirector
        | where SourceContext contains reconciller_name
        | where TIMESTAMP between(start .. end)
        | where message contains search_key
        | project TIMESTAMP, message
        """
        
        try:
            response = client.execute_query(DATABASE_NAME, query, crp)
            for row in response.primary_results[0]:
                result.append(f"Timestamp: {row['TIMESTAMP']}, Message: {row['message']}")
        except Exception as e:
            return f"Error executing query: {str(e)}"

        return "\n---\n".join(result)
    
@mcp.tool()
def get_rp_events(region_name: str, server_name: str, start_time: str, end_time: str) -> str:
    """
    Get resource provider (RP) messages which contains information about all operations on the server between the given time range.
    It also contains information about event types for each operation
    Each operation has a unique request_id which can be used to get more information about the operation.

    Args:
        region_name: Name of the region where server is deployed
        server_name: Name of the server
        start_time: Start time of the query
        end_time: End time of the query
    """
    cluster_uri = region_uris.get(region_name)
    if not cluster_uri:
        return f"Region {region_name} is not supported"
    
    kcsb = KustoConnectionStringBuilder.with_az_cli_authentication(cluster_uri)

    result = []

    with KustoClient(kcsb) as client:
        crp = ClientRequestProperties()
        crp.client_request_id = "kusto-mcp-server" + str(uuid.uuid4())
        crp.set_parameter("target_server_name", server_name)
        crp.set_parameter("start_time", start_time)
        crp.set_parameter("end_time", end_time)

        query = """
        declare query_parameters(target_server_name:string, start_time: string, end_time: string);
        let start = todatetime(start_time);
        let end = todatetime(end_time);
        MonOrcasBreadthRp
        | where operation_parameters has target_server_name
        | where TIMESTAMP between(start .. end)
        | project TIMESTAMP, event, operation_type, request_id
        """
        
        try:
            response = client.execute_query(DATABASE_NAME, query, crp)
            for row in response.primary_results[0]:
                result.append(f"Timestamp: {row['TIMESTAMP']}, Event: {row['event']}, OperationType: {row['operation_type']}, Request ID: {row['request_id']}")
        except Exception as e:
            return f"Error executing query: {str(e)}"

        return "\n---\n".join(result)

@mcp.tool()
def get_rp_events_from_request_id(region_name: str, request_id: str, start_time: str, end_time: str, search_key: str = None) -> str:
    """
    Get resource provider (RP) messages for a particular request id between the given time range.

    Args:
        region_name: Name of the region where server is deployed
        request_id: Request ID of the operation
        start_time: Start time of the query
        end_time: End time of the query
        search_key: Search key to filter messages
    """
    cluster_uri = region_uris.get(region_name)
    if not cluster_uri:
        return f"Region {region_name} is not supported"
    
    kcsb = KustoConnectionStringBuilder.with_az_cli_authentication(cluster_uri)

    result = []

    with KustoClient(kcsb) as client:
        crp = ClientRequestProperties()
        crp.client_request_id = "kusto-mcp-server" + str(uuid.uuid4())
        crp.set_parameter("target_request_id", request_id)
        crp.set_parameter("start_time", start_time)
        crp.set_parameter("end_time", end_time)
        crp.set_parameter("search_key", search_key)

        query = """
        declare query_parameters(target_request_id:string, start_time: string, end_time: string, search_key: string);
        let start = todatetime(start_time);
        let end = todatetime(end_time);
        MonOrcasBreadthRp
        | where request_id == target_request_id
        | where TIMESTAMP between(start .. end)
        | where isnotempty(message) or isnotempty(error_message)
        | where message contains search_key
        | project TIMESTAMP, message, error_message
        """
        
        try:
            response = client.execute_query(DATABASE_NAME, query, crp)
            for row in response.primary_results[0]:
                result.append(f"Timestamp: {row['TIMESTAMP']}, Message: {row['message']}, ErrorMessage: {row['error_message']}")
        except Exception as e:
            return f"Error executing query: {str(e)}"

        return "\n---\n".join(result)
       
if __name__ == "__main__":
    # Run the FastMCP server using the stdio transport
    mcp.run(transport="stdio")