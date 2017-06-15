import APNewsCollector
import ESClient
import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import groovy.json.JsonOutput

/**
 * class for use by AWS Lambda
 */
class APNewsCollectorLambda implements RequestHandler<Request, String> {

    static class Request extends ESClient.Config {
        String clean
    }

    public String handleRequest(Request request, Context context) {
        def esConfig = [
            url  : request.url,
            index: request.index ?: APNewsCollector.DEFAULT_ES_INDEX,
            type : APNewsCollector.DEFAULT_ES_TYPE,
            user : request.user ?: APNewsCollector.DEFAULT_ES_USER,
            pass : request.pass ?: APNewsCollector.DEFAULT_ES_PASS
        ] as ESClient.Config

        return JsonOutput.toJson(APNewsCollector.run(esConfig, request.clean as boolean))
    }
}