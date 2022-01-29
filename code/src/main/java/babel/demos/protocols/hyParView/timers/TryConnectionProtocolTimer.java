package babel.demos.protocols.hyParView.timers;

import babel.timer.ProtocolTimer;
import network.Host;

public class TryConnectionProtocolTimer extends ProtocolTimer {

    public static final short TimerCode = 102;
    private Host tryConnectionNode;

    public TryConnectionProtocolTimer(Host crashedNode) {
        super(TryConnectionProtocolTimer.TimerCode);
        this.tryConnectionNode = crashedNode;
    }

    public Host getTryConnectionNode(){
        return tryConnectionNode;
    }

    @Override
    public Object clone() {
        return this;
    }
}
