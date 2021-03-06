package com.mongodb.resources;

import com.mongodb.services.MongoReactiveStreamServices;
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

@Path("/reactive-stream")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class MongoReactiveStreamResources {

    @Inject
    private MongoReactiveStreamServices mongoReactiveStreamResources;

    @POST
    @Path("/data/find/{collection}")
    public Response getDataFind(
            @Parameter(description = "name of collection", required = true) @PathParam("collection") String collectionName,
            @Parameter(description = "page size") @QueryParam("pageSize") @DefaultValue("1") Integer pageSize,
            @Parameter(description = "page size") @QueryParam("pageNumber") @DefaultValue("100") Integer pageNumber,
            JsonObject payload) {
        List<Map<String, Object>> documents = mongoReactiveStreamResources.getDocsWithCommandFind(collectionName, payload, new Pagination(pageSize, pageNumber));
        Response.ResponseBuilder response = Response.status(Response.Status.OK);
        response.entity(documents);
        return response.build();
    }

    @POST
    @Path("/total/find/{collection}")
    public Response getTotalFind(
            @Parameter(description = "name of collection", required = true) @PathParam("collection") String collectionName,
            JsonObject payload) {
        Long totalDocs = mongoReactiveStreamResources.getTotalDocsCommandFind(collectionName, payload);
        Response.ResponseBuilder response = Response.status(Response.Status.OK);
        response.entity(Collections.singletonMap("total", totalDocs));
        return response.build();
    }

    @POST
    @Path("/data/aggregate/{collection}")
    public Response getDataAggregate(
            @Parameter(description = "name of collection", required = true) @PathParam("collection") String collectionName,
            @Parameter(description = "page size") @QueryParam("pageSize") @DefaultValue("1") Integer pageSize,
            @Parameter(description = "page size") @QueryParam("pageNumber") @DefaultValue("100") Integer pageNumber,
            JsonObject payload) {
        List<Map<String, Object>> documents = mongoReactiveStreamResources.getDocsWithCommandAggregate(collectionName, payload, new Pagination(pageSize, pageNumber));
        Response.ResponseBuilder response = Response.status(Response.Status.OK);
        response.entity(documents);
        return response.build();
    }

    @POST
    @Path("/total/aggregate/{collection}")
    public Response getTotalAggregate(
            @Parameter(description = "name of collection", required = true) @PathParam("collection") String collectionName,
            JsonObject payload) {
        Long totalDocs = mongoReactiveStreamResources.getTotalDocsCommandAggregate(collectionName, payload);
        Response.ResponseBuilder response = Response.status(Response.Status.OK);
        response.entity(Collections.singletonMap("total", totalDocs));
        return response.build();
    }

    @POST
    @Path("/sq-op1/data/find")
    public Response getDataFindWithSubQueryOP1(
            @Parameter(description = "page size") @QueryParam("pageSize") @DefaultValue("1") Integer pageSize,
            @Parameter(description = "page number") @QueryParam("pageNumber") @DefaultValue("100") Integer pageNumber,
            JsonObject payload) {
        List<Map<String, Object>> documents = mongoReactiveStreamResources.getDocsWithCommandFindWithSubQueryOP1(payload, new Pagination(pageSize, pageNumber));
        Response.ResponseBuilder response = Response.status(Response.Status.OK);
        response.entity(documents);
        return response.build();
    }

    @POST
    @Path("/sq-op2/data/find")
    public Response getDataFindWithSubQueryOP2(
            @Parameter(description = "page size") @QueryParam("pageSize") @DefaultValue("1") Integer pageSize,
            @Parameter(description = "page number") @QueryParam("pageNumber") @DefaultValue("100") Integer pageNumber,
            JsonObject payload) {
        List<Map<String, Object>> documents = mongoReactiveStreamResources.getDocsWithCommandFindWithSubQueryOP2(payload, new Pagination(pageSize, pageNumber));
        Response.ResponseBuilder response = Response.status(Response.Status.OK);
        response.entity(documents);
        return response.build();
    }
}
