package babel.demos.protocols.chord.utils;

import babel.demos.protocols.chord.Chord;
import io.netty.buffer.ByteBuf;
import network.Host;
//import network.ISerializer;

import java.math.BigInteger;
import java.net.UnknownHostException;

public class ChordNode {
    BigInteger nodeId;
    Host nodeAddress;

    public ChordNode(BigInteger nodeId, Host nodeAddress) {
        this.nodeId = nodeId;
        this.nodeAddress = nodeAddress;
    }

    public ChordNode(BigInteger nodeId) {
        this.nodeId = nodeId;
    }

    public BigInteger getNodeId() {
        return nodeId;
    }

    public void setNodeId(BigInteger nodeId) {
        this.nodeId = nodeId;
    }

    public Host getNodeAddress() {
        return nodeAddress;
    }

    public boolean equals(ChordNode c1) {
        return nodeAddress.equals(c1.nodeAddress) && (nodeId.compareTo(c1.getNodeId()) == 0);
    }

    public void serialize(ByteBuf out) {
        if (nodeId != null) {
            //System.out.println("[CH - NODE] nodeId " + nodeId);
            byte[] id = nodeId.toByteArray();
            //System.out.println("[CH - NODE] byte[] id" + id);
            out.writeInt(id.length);
            //System.out.println("[CH - NODE] id.lenght" + id.length);
            out.writeBytes(id);
            nodeAddress.serialize(out);
        } else {
            out.writeInt(0);
        }

    }

    public static ChordNode deserialize(ByteBuf in) throws UnknownHostException {
        int nodeIdLength = in.readInt();

        if (nodeIdLength == 0) {
            return new ChordNode(null, null);
        } else {
            byte[] id = new byte[nodeIdLength];
            in.readBytes(id);
            BigInteger nodeId = new BigInteger(id);
            Host nodeAddress = Host.deserialize(in);
            return new ChordNode(nodeId, nodeAddress);
        }
    }

    public int serializedSize() {
        if (nodeId == null)
            return 4;
        else
            return 4 + nodeId.toByteArray().length + nodeAddress.serializedSize();
    }

    public static Boolean intervalOpenClose(ChordNode inf, ChordNode sup, ChordNode middle) {
        BigInteger infId = inf.getNodeId();
        BigInteger supId = sup.getNodeId();

        if (middle == null) {
            return false;
        }

        BigInteger midId = middle.getNodeId();

        if (infId.compareTo(supId) < 0) {
            if (midId.compareTo(infId) > 0 && midId.compareTo(supId) <= 0)
                return true;
            else
                return false;
        } else if (infId.compareTo(supId) > 0){
            if (midId.compareTo(infId) <= 0 || midId.compareTo(supId) > 0)
                return true;
            else
                return false;
        }else { // infId = supId
            return true;
        }
    }

    public static Boolean uniIntervalOpenOpen(ChordNode inf, ChordNode sup, ChordNode middle) {
        BigInteger infId = inf.getNodeId();
        BigInteger supId = sup.getNodeId();

        if (middle == null) {
            return false;
        }

        if (infId == null){
            return true;
        }

        BigInteger midId = middle.getNodeId();

        if (infId.compareTo(supId) < 0) {
            if (midId.compareTo(infId) > 0 && midId.compareTo(supId) < 0)
                return true;
            else
                return false;
        }
        return false;
    }

    public static Boolean uniIntervalOpenClose(ChordNode inf, ChordNode sup, ChordNode middle) {
        BigInteger infId = inf.getNodeId();
        BigInteger supId = sup.getNodeId();

        if (middle == null) {
            return false;
        }

        if (infId == null){
            return true;
        }

        BigInteger midId = middle.getNodeId();

        if (infId.compareTo(supId) < 0) {
            if (midId.compareTo(infId) > 0 && midId.compareTo(supId) <= 0)
                return true;
            else
                return false;
        }
        return false;
    }

    public static Boolean intervalOpenOpen(ChordNode inf, ChordNode sup, ChordNode middle) {
        BigInteger infId = inf.getNodeId();
        BigInteger supId = sup.getNodeId();
        BigInteger midId = middle.getNodeId();

        if (midId == null) {
            return false;
        }

        if (infId.compareTo(supId) < 0) {
            if (midId.compareTo(infId) > 0 && midId.compareTo(supId) < 0)
                return true;
            else
                return false;
        } else {
            if (midId.compareTo(infId) < 0 || midId.compareTo(supId) > 0)
                return true;
            else
                return false;
        }
    }
}

