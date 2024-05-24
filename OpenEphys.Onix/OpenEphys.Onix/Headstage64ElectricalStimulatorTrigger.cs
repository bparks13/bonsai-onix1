﻿using System;
using System.ComponentModel;
using System.Drawing.Design;
using System.Linq;
using System.Reactive;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using Bonsai;

namespace OpenEphys.Onix
{
    public class Headstage64ElectricalStimulatorTrigger: Sink<bool>
    {
        readonly BehaviorSubject<bool> enable = new(true);
        readonly BehaviorSubject<double> phaseOneCurrent = new(0);
        readonly BehaviorSubject<double> interPhaseCurrent = new(0);
        readonly BehaviorSubject<double> phaseTwoCurrent = new(0);
        readonly BehaviorSubject<uint> phaseOneDuration = new(0);
        readonly BehaviorSubject<uint> interPhaseInterval = new(0);
        readonly BehaviorSubject<uint> phaseTwoDuration = new(0);
        readonly BehaviorSubject<uint> interPulseInterval = new(0);
        readonly BehaviorSubject<uint> burstPulseCount = new(0);
        readonly BehaviorSubject<uint> interBurstInterval = new(0);
        readonly BehaviorSubject<uint> trainBurstCount = new(0);
        readonly BehaviorSubject<uint> trainDelay = new(0);
        readonly BehaviorSubject<bool> powerEnable = new(false);

        const double DacBitDepth = 16;
        const double AbsMaxMicroAmps = 2500;

        [TypeConverter(typeof(Headstage64ElectricalStimulator.NameConverter))]
        public string DeviceName { get; set; }

        [Description("Specifies whether the electrical stimulation subcircuit will respect triggers.")]
        public bool Enable
        {
            get => enable.Value;
            set => enable.OnNext(value);
        }

        [Description("Phase 1 pulse current (uA).")]
        [Range(-AbsMaxMicroAmps, AbsMaxMicroAmps)]
        [Editor(DesignTypes.SliderEditor, typeof(UITypeEditor))]
        [Precision(3, 1)]
        public double PhaseOneCurrent
        {
            get => phaseOneCurrent.Value;
            set => phaseOneCurrent.OnNext(value);
        }

        [Description("Interphase rest current (uA).")]
        [Range(-AbsMaxMicroAmps, AbsMaxMicroAmps)]
        [Editor(DesignTypes.SliderEditor, typeof(UITypeEditor))]
        [Precision(3, 1)]
        public double InterPhaseCurrent
        {
            get => interPhaseCurrent.Value;
            set => interPhaseCurrent.OnNext(value);
        }

        [Description("Phase 2 pulse current (uA).")]
        [Range(-AbsMaxMicroAmps, AbsMaxMicroAmps)]
        [Editor(DesignTypes.SliderEditor, typeof(UITypeEditor))]
        [Precision(3, 1)]
        public double PhaseTwoCurrent
        {
            get => phaseTwoCurrent.Value;
            set => phaseTwoCurrent.OnNext(value);
        }

        [Description("Pulse train start delay (uSec).")]
        [Range(0, uint.MaxValue)]
        public uint TrainDelay
        {
            get => trainDelay.Value;
            set => trainDelay.OnNext(value);
        }

        [Description("Phase 1 pulse duration (uSec).")]
        [Range(0, uint.MaxValue)]
        public uint PhaseOneDuration
        {
            get => phaseOneDuration.Value;
            set => phaseOneDuration.OnNext(value);
        }

        [Description("Inter-phase interval (uSec).")]
        [Range(0, uint.MaxValue)]
        public uint InterPhaseInterval
        {
            get => interPhaseInterval.Value;
            set => interPhaseInterval.OnNext(value);
        }

        [Description("Phase 2 pulse duration (uSec).")]
        [Range(0, uint.MaxValue)]
        public uint PhaseTwoDuration
        {
            get => phaseTwoDuration.Value;
            set => phaseTwoDuration.OnNext(value);
        }

        [Description("Inter-pulse interval (uSec).")]
        [Range(0, uint.MaxValue)]
        public uint InterPulseInterval
        {
            get => interPulseInterval.Value;
            set => interPulseInterval.OnNext(value);
        }

        [Description("Inter-burst interval (uSec).")]
        [Range(0, uint.MaxValue)]
        public uint InterBurstInterval
        {
            get => interBurstInterval.Value;
            set => interBurstInterval.OnNext(value);
        }

        [Description("Number of pulses in each burst.")]
        [Range(0, uint.MaxValue)]
        public uint BurstPulseCount
        {
            get => burstPulseCount.Value;
            set => burstPulseCount.OnNext(value);
        }

        [Description("Number of bursts in each train.")]
        [Range(0, uint.MaxValue)]
        public uint TrainBurstCount
        {
            get => trainBurstCount.Value;
            set => trainBurstCount.OnNext(value);
        }

        [Description("Stimulator power on/off.")]
        [Range(0, uint.MaxValue)]
        public bool PowerEnable
        {
            get => powerEnable.Value;
            set => powerEnable.OnNext(value);
        }

        public override IObservable<bool> Process(IObservable<bool> source)
        {
            return Observable.Using(
                () => DeviceManager.ReserveDevice(DeviceName),
                disposable => disposable.Subject.SelectMany(deviceInfo =>
                    Observable.Create<bool>(observer =>
                    {
                        var device = deviceInfo.GetDeviceContext(typeof(Headstage64ElectricalStimulator));
                        var triggerObserver = Observer.Create<bool>(
                            value => device.WriteRegister(Headstage64ElectricalStimulator.TRIGGER, value ? 1u : 0u),
                            observer.OnError,
                            observer.OnCompleted);

                        static uint uAToCode(double currentuA)
                        {
                            var k = 1 / (2 * AbsMaxMicroAmps / (Math.Pow(2, DacBitDepth) - 1)); // static
                            return (uint)(k * (currentuA + AbsMaxMicroAmps));
                        }

                        return new CompositeDisposable(
                            enable.Subscribe(value => device.WriteRegister(Headstage64ElectricalStimulator.ENABLE, value ? 1u : 0u)),
                            phaseOneCurrent.Subscribe(value => device.WriteRegister(Headstage64ElectricalStimulator.CURRENT1, uAToCode(value))),
                            interPhaseCurrent.Subscribe(value => device.WriteRegister(Headstage64ElectricalStimulator.RESTCURR, uAToCode(value))),
                            phaseTwoCurrent.Subscribe(value => device.WriteRegister(Headstage64ElectricalStimulator.CURRENT2, uAToCode(value))),
                            trainDelay.Subscribe(value => device.WriteRegister(Headstage64ElectricalStimulator.TRAINDELAY, value)),
                            phaseOneDuration.Subscribe(value => device.WriteRegister(Headstage64ElectricalStimulator.PULSEDUR1, value)),
                            interPhaseInterval.Subscribe(value => device.WriteRegister(Headstage64ElectricalStimulator.INTERPHASEINTERVAL, value)),
                            phaseTwoDuration.Subscribe(value => device.WriteRegister(Headstage64ElectricalStimulator.PULSEDUR2, value)),
                            interPulseInterval.Subscribe(value => device.WriteRegister(Headstage64ElectricalStimulator.INTERPULSEINTERVAL, value)),
                            interBurstInterval.Subscribe(value => device.WriteRegister(Headstage64ElectricalStimulator.INTERBURSTINTERVAL, value)),
                            burstPulseCount.Subscribe(value => device.WriteRegister(Headstage64ElectricalStimulator.BURSTCOUNT, value)),
                            trainBurstCount.Subscribe(value => device.WriteRegister(Headstage64ElectricalStimulator.TRAINCOUNT, value)),
                            powerEnable.Subscribe(value => device.WriteRegister(Headstage64ElectricalStimulator.POWERON, value ? 1u : 0u)),
                            source.SubscribeSafe(triggerObserver)
                        );
                    })));
        }
    }
}
