package com.mariner;

public class Report {

    private String client_address;
    private String client_guid;
    private String request_time;
    private String service_guid;
    private Integer retries_request;
    private Integer packets_requested;
    private Integer packets_serviced;
    private Integer max_hole_size;
    private static String[] columns;

    public Report(){
    }

    public static String[] setColumns(String csvColumnNames )
    {
        columns = csvColumnNames.split(",");
        return columns;
    }

    public static String[] getColumns()
    {
        return columns;
    }

    public String getClient_address() {
        return client_address;
    }
    public void setClient_address(String client_address) {
        this.client_address = client_address;
    }
    public String getClient_guid() {
        return client_guid;
    }
    public void setClient_guid(String client_guid) {
        this.client_guid = client_guid;
    }
    public String getRequest_time() {
        return request_time;
    }
    public void setRequest_time(String request_time) {
        this.request_time = request_time;
    }
    public String getService_guid() {
        return service_guid;
    }
    public void setService_guid(String service_guid) {
        this.service_guid = service_guid;
    }
    public Integer getRetries_request() {return retries_request;}
    public void setRetries_request(Integer retries_request) {
        this.retries_request = retries_request;
    }
    public Integer getPackets_requested() {
        return packets_requested;
    }
    public void setPackets_requested(Integer packets_requested) {
        this.packets_requested = packets_requested;
    }
    public Integer getPackets_serviced() {
        return packets_serviced;
    }
    public void setPackets_serviced(Integer packets_serviced) {
        this.packets_serviced = packets_serviced;
    }
    public Integer getMax_hole_size() {
        return max_hole_size;
    }
    public void setMax_hole_size(Integer max_hole_size) {
        this.max_hole_size = max_hole_size;
    }
}
