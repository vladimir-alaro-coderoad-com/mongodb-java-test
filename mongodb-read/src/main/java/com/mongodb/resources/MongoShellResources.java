package com.mongodb.resources;

import com.mongodb.services.MongoShellServices;
import com.mongodb.util.Pagination;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;

import javax.inject.Inject;
import javax.json.JsonObject;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.Map;

@Path("/mongo-shell")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class MongoShellResources {

    @Inject
    private MongoShellServices mongoShellServices;

    @POST
    @Path("/sq-op1/data/find")
    public Response getDataFindWithSubQueryOP1(
            @Parameter(description = "page size") @QueryParam("pageSize") @DefaultValue("1") Integer pageSize,
            @Parameter(description = "page number") @QueryParam("pageNumber") @DefaultValue("100") Integer pageNumber,
            JsonObject payload) throws Exception {
        List<Map<String, Object>> documents = mongoShellServices.getDocsWithCommandFindWithSubQueryOP1(payload, new Pagination(pageSize, pageNumber));
        Response.ResponseBuilder response = Response.status(Response.Status.OK);
        response.entity(documents);
        return response.build();
    }

    @POST
    @Path("/sq-op2/data/aggregate")
    public Response getDataFindWithSubQueryOP2(
            @Parameter(description = "page size") @QueryParam("pageSize") @DefaultValue("1") Integer pageSize,
            @Parameter(description = "page number") @QueryParam("pageNumber") @DefaultValue("100") Integer pageNumber,
            JsonObject payload) throws Exception {
        List<Map<String, Object>> documents = mongoShellServices.getDocsWithCommandFindWithSubQueryOP2(payload, new Pagination(pageSize, pageNumber));
        Response.ResponseBuilder response = Response.status(Response.Status.OK);
        response.entity(documents);
        return response.build();
    }
}

