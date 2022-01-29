package babel.demos.protocols.chord.timers;

import babel.timer.ProtocolTimer;

public class FixFingersProtocolTimer extends ProtocolTimer {

    public static final short TimerCode = 502;

    public FixFingersProtocolTimer() {
        super(FixFingersProtocolTimer.TimerCode);
    }

    @Override
    public Object clone() {
        return this;
    }
}
