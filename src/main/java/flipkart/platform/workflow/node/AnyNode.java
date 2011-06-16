package flipkart.platform.workflow.node;

/**
 * Wraps a {@link Node} to facilitate runtime workflow creation. If enough
 * information provided, can perform type checking for fast failures.
 * 
 * @param <I>
 *            Input Job description type
 * @param <O>
 *            Output Job description type
 */
public class AnyNode<I, O>
{
    private final Node<I, O> node;
    private final Class<I> iClass;
    private final Class<O> oClass;

    /**
     * Class constructor that provides no runtime checks when binding nodes.
     * 
     * @see AnyNode
     * 
     * @param node
     *            Node
     */
    public AnyNode(Node<I, O> node)
    {
        this(node, null, null);
    }

    /**
     * Class constructor that provides runtime type checks in
     * {@link #acceptAny(Object)} and {@link #acceptAny(Object)} to verify the
     * correctness of the arguments.
     * 
     * @param node
     *            Node to be wrapped
     * @param iClass
     *            Input type class
     * @param oClass
     *            Output type class
     */
    public AnyNode(Node<I, O> node, Class<I> iClass, Class<O> oClass)
    {
        this.node = node;
        this.iClass = iClass;
        this.oClass = oClass;
    }

    public String getName()
    {
        return node.getName();
    }

    /**
     * Append any node. If input and output job class information is provided
     * using {@link #AnyNode(Node, Class, Class)}, additional runtime checks are
     * performed to ensure if this node is compatible with given node.
     * 
     * @param otherNode
     *            Node to be appened to this node
     * @throws TypeMismatchException
     */
    @SuppressWarnings("unchecked")
    public <T> void appendAny(AnyNode<T, ?> otherNode)
            throws TypeMismatchException
    {
        if (this.oClass != null && otherNode.iClass != null
                && !this.oClass.isAssignableFrom(otherNode.iClass))
        {
            throw new TypeMismatchException("Output type ("
                    + this.oClass.getName() + ") of node: " + this.getName()
                    + " does not match input type ("
                    + otherNode.iClass.getName() + ") of node: "
                    + otherNode.getName());
        }
        node.append((Node<O, ?>) otherNode.node);
    }

    /**
     * Accepts any value as input for the wrapped node. If input job class
     * information is provided using {@link #AnyNode(Node, Class, Class)}, will
     * determine if given object can be converted to expected type.
     * 
     * @param i
     *            Input job description
     * @throws TypeMismatchException
     */
    @SuppressWarnings("unchecked")
    public <T> void acceptAny(T i) throws TypeMismatchException
    {
        if (iClass != null)
        {
            try
            {
                node.accept(iClass.cast(i));
            }
            catch (ClassCastException e)
            {
                throw new TypeMismatchException("Expected type: "
                        + this.iClass.getName() + "; got input of type: "
                        + i.getClass().getName(), e);
            }
        }
        else
        {
            node.accept((I) i);
        }
    }

    public void shutdown(boolean awaitTerminataion) throws InterruptedException
    {
        node.shutdown(awaitTerminataion);
    }
}
