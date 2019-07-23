package com.mongodb.resources;

import com.mongodb.services.AsynchronousDBServices;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;

import javax.inject.Inject;
import javax.json.JsonObject;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@Path("/async")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class AsynchronousResources {

    @Inject
    private AsynchronousDBServices asynchronousDBServices;

    @POST
    @Path("/data/find/{collection}")
    public Response getDataFind(
            @Parameter(description = "name of collection", required = true) @PathParam("collection") String collectionName,
            JsonObject payload) {
        List<Map<String, Object>> documents = asynchronousDBServices.getDocsWithCommandFind(collectionName, payload);
        Response.ResponseBuilder response = Response.status(Response.Status.OK);
        response.entity(documents);
        return response.build();
    }

    @POST
    @Path("/total/find/{collection}")
    public Response getTotalFind(
            @Parameter(description = "name of collection", required = true) @PathParam("collection") String collectionName,
            JsonObject payload) {
        Long totalDocs = asynchronousDBServices.getTotalDocsCommandFind(collectionName, payload);
        Response.ResponseBuilder response = Response.status(Response.Status.OK);
        response.entity(Collections.singletonMap("total", totalDocs));
        return response.build();
    }

    @POST
    @Path("/data/aggregate/{collection}")
    public Response getDataAggregate(
            @Parameter(description = "collection's name", required = true) @PathParam("collection") String collectionName,
            JsonObject payload) {
        List<Map<String, Object>> documents = asynchronousDBServices.getDocsWithCommandAggregate(collectionName, payload);
        Response.ResponseBuilder response = Response.status(Response.Status.OK);
        response.entity(documents);
        return response.build();
    }

    @POST
    @Path("/total/aggregate/{collection}")
    public Response getTotalAggregate(
            @Parameter(description = "name of collection", required = true) @PathParam("collection") String collectionName,
            JsonObject payload) {
        Long totalDocs = asynchronousDBServices.getTotalDocsCommandAggregate(collectionName, payload);
        Response.ResponseBuilder response = Response.status(Response.Status.OK);
        response.entity(Collections.singletonMap("total", totalDocs));
        return response.build();
    }
}
