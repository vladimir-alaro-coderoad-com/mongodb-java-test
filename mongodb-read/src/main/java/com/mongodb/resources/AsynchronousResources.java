package com.mongodb.resources;

import com.mongodb.services.AsynchronousDBServices;
import com.mongodb.util.Pagination;
import org.bson.Document;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;

import javax.inject.Inject;
import javax.json.JsonObject;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

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
            @Parameter(description = "page size") @QueryParam("pageSize") @DefaultValue("1") Integer pageSize,
            @Parameter(description = "page size") @QueryParam("pageNumber") @DefaultValue("100") Integer pageNumber,
            JsonObject payload) {
        Pagination pagination = new Pagination(pageSize, pageNumber);
        List<Map<String, Object>> documents = asynchronousDBServices.getDocsWithCommandFind(collectionName, payload, pagination);
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
            @Parameter(description = "page size") @QueryParam("pageSize") @DefaultValue("1") Integer pageSize,
            @Parameter(description = "page size") @QueryParam("pageNumber") @DefaultValue("100") Integer pageNumber,
            JsonObject payload) {
        Pagination pagination = new Pagination(pageSize, pageNumber);
        List<Map<String, Object>> documents = asynchronousDBServices.getDocsWithCommandAggregate(collectionName, payload, pagination);
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

    @POST
    @Path("/total/functions")
    public Response totalFunctions() throws ExecutionException, InterruptedException {
        int totalDocs = asynchronousDBServices.getTotalFunctions();
        Response.ResponseBuilder response = Response.status(Response.Status.OK);
        response.entity(Collections.singletonMap("total", totalDocs));
        return response.build();
    }

    @PUT
    @Path("function/{name}/{collection}")
    public Response createFunction(
            @Parameter(description = "function's name", required = true) @PathParam("name") String functionName,
            @Parameter(description = "collection's name", required = true) @PathParam("name") String collectionName,
            JsonObject payload) {
        return Response.status(Response.Status.NOT_IMPLEMENTED).build();
    }

    @PATCH
    @Path("function/{name}/{collection}")
    public Response updateFunction(
            @Parameter(description = "function's name", required = true) @PathParam("name") String functionName,
            @Parameter(description = "collection's name", required = true) @PathParam("name") String collectionName,
            JsonObject payload) {
        return Response.status(Response.Status.NOT_IMPLEMENTED).build();
    }

    @DELETE
    @Path("function/{name}/{collection}")
    public Response deleteFunction(
            @Parameter(description = "function's name", required = true) @PathParam("name") String functionName,
            @Parameter(description = "collection's name", required = true) @PathParam("name") String collectionName,
            JsonObject payload) {
        return Response.status(Response.Status.NOT_IMPLEMENTED).build();
    }

    @POST
    @Path("/run/function/{name}/{collection}")
    public Response run(
            @Parameter(description = "function's name", required = true) @PathParam("name") String functionName,
            @Parameter(description = "collection's name", required = true) @PathParam("name") String collectionName,
            JsonObject payload) throws ExecutionException, InterruptedException {
        Document result = asynchronousDBServices.runFunction();
        Response.ResponseBuilder response = Response.status(Response.Status.OK);
        response.entity(result);
        return response.build();
    }
}
