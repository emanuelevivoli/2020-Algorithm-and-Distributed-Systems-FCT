package babel.demos.protocols.scribe;

import network.Host;

import java.util.HashSet;
import java.util.Set;

public class TopicManager {

    protected String topic;

    protected Set<Host> children;

    protected Host root;

    protected boolean subscribed;

    public TopicManager(String topic){
        this.topic = topic;
        this.subscribed = false;
        this.children = new HashSet<>();
    }

    public Host setRoot(Host h){
        Host lastRoot = this.root;
        this.root = h;

        return lastRoot;
    }

    public Host getRoot(){
        return this.root;
    }

    public boolean isRoot(Host h){
        if(this.root == null) return false;
        return root.toString().equals(h.toString());
    }

    public void addChildren(Host h){
        if(h == null) return;

        if(!children.contains(h)) {
            children.add(h);
        }
    }

    public Set<Host> getChildren(){
        return this.children;
    }

    public int getChildrenSize() { return this.children.size(); }

    /**
     * @param h host to remove
     * @return true if there are no more children (we can unsub from the root)
     */
    public void removeChildren(Host h){
        children.remove(h);
    }

    public void setSubscription(boolean subscribed){
        this.subscribed = subscribed;
    }

    public boolean amISubscribed(){
        return this.subscribed;
    }

}