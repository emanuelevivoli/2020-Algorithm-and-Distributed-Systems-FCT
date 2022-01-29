package babel.demos.protocols.scribe.timers;

import babel.timer.ProtocolTimer;

public class RepairTreeProtocolTimer extends ProtocolTimer {

    public static final short TimerCode = 109;

    public RepairTreeProtocolTimer() {
        super(RepairTreeProtocolTimer.TimerCode);
    }

    @Override
    public Object clone() {
        return this;
    }
}
