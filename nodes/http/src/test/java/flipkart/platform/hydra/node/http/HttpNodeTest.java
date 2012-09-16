package flipkart.platform.hydra.node.http;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Request;
import com.ning.http.client.Response;
import flipkart.platform.hydra.job.ExecutionFailureException;
import flipkart.platform.hydra.jobs.HttpJob;
import flipkart.platform.hydra.node.BaseNode;
import flipkart.platform.hydra.node.Node;
import flipkart.platform.hydra.node.builder.HttpNodeBuilder;
import flipkart.platform.hydra.topology.LinkTopology;
import org.apache.commons.lang.RandomStringUtils;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static flipkart.platform.hydra.link.LinkBuilder.link;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * User: shashwat
 * Date: 12/08/12
 */
public class HttpNodeTest
{
    private static Server server;

    public static class HelloHandler extends AbstractHandler
    {
        @Override
        public void handle(String target, org.eclipse.jetty.server.Request baseRequest, HttpServletRequest request,
            HttpServletResponse response) throws IOException, ServletException
        {
            response.setContentType("text/html;charset=utf-8");
            response.setStatus(HttpServletResponse.SC_OK);
            
            baseRequest.setHandled(true);
            final String q = request.getParameter("q");
            response.getWriter().print("Hello " + q);
        }
    }

    @BeforeClass
    public static void setUp() throws Exception
    {
        server = new Server(20000);
        server.setHandler(new HelloHandler());
        server.start();
    }

    @AfterClass
    public static void tearDown() throws Exception
    {
        server.stop();
    }

    public static class HttpTestJob implements HttpJob<String, String>
    {
        @Override
        public Request buildRequest(AsyncHttpClient client, String s)
        {
            return client.prepareGet("http://localhost:20000?q=" + s).build();
        }

        @Override
        public String buildResponse(String s, Response response) throws ExecutionFailureException
        {
            if (response.getStatusCode() == 200)
            {
                try
                {
                    final String responseBody = response.getResponseBody();
                    System.out.println(responseBody);
                    return responseBody;
                }
                catch (IOException e)
                {
                    throw new ExecutionFailureException("failed to get response stream, cause: ", e);
                }
            }
            throw new ExecutionFailureException("Bad response code " + response.getStatusCode() + ", cause: ");
        }

        @Override
        public void failed(String s, Throwable cause)
        {
            System.out.println(cause);
            assertFalse("Failed: " + s + " cause: " + cause.getMessage(), true);
        }
    }

    public static class ResponseNode extends BaseNode<String, String>
    {
        public String s = null;

        protected ResponseNode()
        {
            super(RandomStringUtils.random(10));
        }

        @Override
        protected void shutdownResources(boolean awaitTermination) throws InterruptedException
        {
        }

        @Override
        protected void acceptMessage(String s)
        {
            this.s = s;
        }
    }

    @Test
    public void testHttpNode() throws Exception
    {
        final HttpNodeBuilder<String, String> nodeBuilder = HttpNodeBuilder.with(HttpTestJob.class);
        nodeBuilder.setFollowRedirects(true);
        nodeBuilder.setMaximumNumberOfRedirects(10);
        final Node<String, String> node = nodeBuilder.build();

        final ResponseNode responseNode = new ResponseNode();
        
        final LinkTopology topology = new LinkTopology();
        link(topology, node).toOnly(responseNode);

        node.accept("world");
        topology.shutdown(true);

        assertEquals("Response should be hello world", "Hello world", responseNode.s);
    }

    // TODO: test retry + retry expiry - on timeout, connection failure and conversion of response to output failure

    // TODO: test discard on exception
}
