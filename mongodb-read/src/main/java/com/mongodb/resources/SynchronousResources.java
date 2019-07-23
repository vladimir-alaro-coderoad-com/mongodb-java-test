package com.mongodb.resources;

import com.mongodb.services.SynchronousDBServices;
import com.mongodb.util.Pagination;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;

import javax.inject.Inject;
import javax.json.JsonObject;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@Path("/sync")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class SynchronousResources {

    @Inject
    private SynchronousDBServices synchronousDBServices;

    @POST
    @Path("/data/find/{collection}")
    public Response getDataFind(
            @Parameter(description = "name of collection", required = true) @PathParam("collection") String collectionName,
            @Parameter(description = "page size") @QueryParam("pageSize") @DefaultValue("1") Integer pageSize,
            @Parameter(description = "page size") @QueryParam("pageNumber") @DefaultValue("100") Integer pageNumber,
            JsonObject payload) {
        Pagination pagination = new Pagination(pageSize, pageNumber);
        List<Map<String, Object>> documents = synchronousDBServices.getDocsWithCommandFind(collectionName, payload, pagination);
        Response.ResponseBuilder response = Response.status(Response.Status.OK);
        response.entity(documents);
        return response.build();
    }

    @POST
    @Path("/total/find/{collection}")
    public Response getTotalFind(
            @Parameter(description = "name of collection", required = true) @PathParam("collection") String collectionName,
            JsonObject payload) {
        Long totalDocs = synchronousDBServices.getTotalDocsCommandFind(collectionName, payload);
        Response.ResponseBuilder response = Response.status(Response.Status.OK);
        response.entity(Collections.singletonMap("total", totalDocs));
        return response.build();
    }

    @POST
    @Path("/data/aggregate/{collection}")
    public Response getDataAggregate(
            @Parameter(description = "collection's name", required = true) @PathParam("collection") String collectionName,
            @Parameter(description = "page size") @QueryParam("pageSize") @DefaultValue("1") Integer pageSize,
            @Parameter(description = "page size") @QueryParam("pageNumber") @DefaultValue("100") Integer pageNumber,
            JsonObject payload) {
        Pagination pagination = new Pagination(pageSize, pageNumber);
        List<Map<String, Object>> documents = synchronousDBServices.getDocsWithCommandAggregate(collectionName, payload, pagination);
        Response.ResponseBuilder response = Response.status(Response.Status.OK);
        response.entity(documents);
        return response.build();
    }

    @POST
    @Path("/total/aggregate/{collection}")
    public Response getTotalAggregate(
            @Parameter(description = "name of collection", required = true) @PathParam("collection") String collectionName,
            JsonObject payload) {
        Long totalDocs = synchronousDBServices.getTotalDocsCommandAggregate(collectionName, payload);
        Response.ResponseBuilder response = Response.status(Response.Status.OK);
        response.entity(Collections.singletonMap("total", totalDocs));
        return response.build();
    }
}
