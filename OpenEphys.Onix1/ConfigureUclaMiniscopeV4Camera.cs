﻿using System;
using System.ComponentModel;
using System.Drawing.Design;
using System.Reactive.Disposables;
using System.Reactive.Subjects;
using System.Threading;
using System.Xml.Serialization;
using Bonsai;

namespace OpenEphys.Onix1
{
    /// <summary>
    /// Configures the camera on a UCLA Miniscope V4.
    /// </summary>
    public class ConfigureUclaMiniscopeV4Camera : SingleDeviceFactory
    {
        readonly BehaviorSubject<double> ledBrightness = new(0);
        readonly BehaviorSubject<UclaMiniscopeV4SensorGain> sensorGain = new(UclaMiniscopeV4SensorGain.Low);
        readonly BehaviorSubject<double> liquidLensVoltage = new(47); // NB: middle of range

        /// <summary>
        /// Initialize a new instance of a <see cref="ConfigureUclaMiniscopeV4Camera"/> class.
        /// </summary>
        public ConfigureUclaMiniscopeV4Camera()
            : base(typeof(UclaMiniscopeV4))
        {
        }

        /// <summary>
        /// Gets or sets a value indicating whether the camera will produce image data.
        /// </summary>
        /// <remarks>
        /// If set to true, <see cref="UclaMiniscopeV4CameraData"/> will produce image data. If set to false, <see
        /// cref="UclaMiniscopeV4CameraData"/> will not produce image data.
        /// </remarks>
        [Category(ConfigurationCategory)]
        [Description("Specifies whether the camera is enabled.")]
        public bool Enable { get; set; } = true;

        /// <summary>
        /// Gets or sets the camera video rate in frames per second.
        /// </summary>
        [Category(ConfigurationCategory)]
        [Description("Camera video rate in frames per second.")]
        public UclaMiniscopeV4FramesPerSecond FrameRate { get; set; } = UclaMiniscopeV4FramesPerSecond.Fps30Hz;

        /// <summary>
        /// Gets or sets the camera sensor's analog gain.
        /// </summary>
        [Description("Camera sensor analog gain.")]
        [Category(AcquisitionCategory)]
        [Editor(DesignTypes.SliderEditor, typeof(UITypeEditor))]
        public UclaMiniscopeV4SensorGain SensorGain
        {
            get => sensorGain.Value;
            set => sensorGain.OnNext(value);
        }

        /// <summary>
        /// Gets or sets a value indicating whether the excitation LED should turn on only when the camera
        /// shutter is open.
        /// </summary>
        /// <remarks>
        /// If set to true, the excitation LED will turn on briefly before, and turn off briefly after, the
        /// camera begins photon collection on its photodiode array. If set to false, the excitation LED will
        /// remain on at all times.
        /// </remarks>
        [Category(ConfigurationCategory)]
        [Description("Only turn on excitation LED during camera exposures.")]
        public bool InterleaveLed { get; set; } = false;

        /// <summary>
        /// Gets or sets the excitation LED brightness level (0-100%).
        /// </summary>
        [Description("Excitation LED brightness level (0-100%).")]
        [Category(AcquisitionCategory)]
        [Range(0, 100)]
        [Precision(1, 1)]
        [Editor(DesignTypes.SliderEditor, typeof(UITypeEditor))]
        public double LedBrightness
        {
            get => ledBrightness.Value;
            set => ledBrightness.OnNext(value);
        }

        /// <summary>
        /// Gets or sets the liquid lens driver voltage (Volts RMS).
        /// </summary>
        /// <remarks>
        /// The imaging focal plane is controlled by using a MAX14574 high-voltage liquid lens driver. This
        /// chip produces pulse-width modulated, 5 kHz alternative electric field that deforms the miniscope's
        /// liquid lens in order to change the focal plane. The strength of this field determines the degree
        /// of deformation and therefore the focal depth. The default setting of 47 Volts RMS corresponds to
        /// approximately mid-range.
        /// </remarks>
        [Description("Liquid lens driver voltage (Volts RMS).")]
        [Category(AcquisitionCategory)]
        [Range(24.4, 69.7)]
        [Precision(1, 1)]
        [Editor(DesignTypes.SliderEditor, typeof(UITypeEditor))]
        public double LiquidLensVoltage
        {
            get => liquidLensVoltage.Value;
            set => liquidLensVoltage.OnNext(value);
        }

        // This is a hack. The hardware is quite unreliable and requires special assistance in order to
        // operate. We do a lot of retries to make this happen. Once we have successful configuration, redoing
        // it can cause failure. Therefore, we allow ConfigureMiniscopeV4 to set this variable in order to
        // skip redundant configuration in Process(). This is not a great solution.
        [XmlIgnore]
        internal bool Configured { get; set; } = false;

        /// <summary>
        /// Configures the camera on a UCLA Miniscope V4.
        /// </summary>
        /// <remarks>
        /// This will schedule configuration actions to be applied by a <see cref="StartAcquisition"/> node
        /// prior to data acquisition.
        /// </remarks>
        /// <param name="source">A sequence of <see cref="ContextTask"/> instances that holds all
        /// configuration actions.</param>
        /// <returns>
        /// The original sequence but with each <see cref="ContextTask"/> instance now containing
        /// configuration actions required to use the miniscope's camera.
        /// </returns>
        public override IObservable<ContextTask> Process(IObservable<ContextTask> source)
        {
            var enable = Enable;
            var deviceName = DeviceName;
            var deviceAddress = DeviceAddress;
            var frameRate = FrameRate;
            var interleaveLed = InterleaveLed;

            return source.ConfigureDevice(context =>
            {
                try
                {
                    // configure device via the DS90UB9x deserializer device
                    var device = context.GetPassthroughDeviceContext(deviceAddress, typeof(DS90UB9x));

                    // TODO: early exit if false?
                    device.WriteRegister(DS90UB9x.ENABLE, enable ? 1u : 0);

                    // check if configuration was already performed on this object
                    if (!Configured)
                    {
                        ConfigureDeserializer(device);
                        ConfigureCameraSystem(device, frameRate, interleaveLed);
                    }

                    var deviceInfo = new DeviceInfo(context, DeviceType, deviceAddress);
                    var shutdown = Disposable.Create(() =>
                    {
                        // turn off EWL
                        var max14574 = new I2CRegisterContext(device, UclaMiniscopeV4.Max14574Address);
                        max14574.WriteByte(0x03, 0x00);

                        // turn off LED
                        var atMega = new I2CRegisterContext(device, UclaMiniscopeV4.AtMegaAddress);
                        atMega.WriteByte(1, 0xFF);

                        //Turn off image sensor, recommended by the datasheet to avoid transients at power off
                        ShutDownCamera(device);
                    });
                    return new CompositeDisposable(
                        ledBrightness.Subscribe(value => SetLedBrightness(device, value)),
                        sensorGain.Subscribe(value => SetSensorGain(device, value)),
                        liquidLensVoltage.Subscribe(value => SetLiquidLensVoltage(device, value)),
                        DeviceManager.RegisterDevice(deviceName, deviceInfo),
                        shutdown);
                }
                finally
                {
                    // needs to be reconfigured past this point, cannot rely on putting this action in
                    // shutdown disposable since any exception before this function returns will not enforce
                    // this setting
                    Configured = false;
                }
            });
        }

        internal static void ConfigureDeserializer(DeviceContext device)
        {
            // configure deserializer
            device.WriteRegister(DS90UB9x.TRIGGEROFF, 0);
            device.WriteRegister(DS90UB9x.READSZ, UclaMiniscopeV4.SensorColumns);
            device.WriteRegister(DS90UB9x.TRIGGER, (uint)DS90UB9xTriggerMode.HsyncEdgePositive);
            device.WriteRegister(DS90UB9x.SYNCBITS, 0);
            device.WriteRegister(DS90UB9x.DATAGATE, (uint)DS90UB9xDataGate.VsyncPositive);

            // NB: This is required because Bonsai is not guaranteed to capture every frame at the start of
            // acquisition. For this reason, the frame start needs to be marked.
            device.WriteRegister(DS90UB9x.MARK, (uint)DS90UB9xMarkMode.VsyncRising);

            DS90UB9x.Initialize933SerDesLink(device, DS90UB9xMode.Raw12BitLowFrequency);

            var deserializer = new I2CRegisterContext(device, DS90UB9x.DES_ADDR);
     
            uint i2cAlias = UclaMiniscopeV4.AtMegaAddress << 1;
            deserializer.WriteByte((uint)DS90UB9xDeserializerI2CRegister.SlaveID1, i2cAlias);
            deserializer.WriteByte((uint)DS90UB9xDeserializerI2CRegister.SlaveAlias1, i2cAlias);

            i2cAlias = UclaMiniscopeV4.Tpl0102Address << 1;
            deserializer.WriteByte((uint)DS90UB9xDeserializerI2CRegister.SlaveID2, i2cAlias);
            deserializer.WriteByte((uint)DS90UB9xDeserializerI2CRegister.SlaveAlias2, i2cAlias);

            i2cAlias = UclaMiniscopeV4.Max14574Address << 1;
            deserializer.WriteByte((uint)DS90UB9xDeserializerI2CRegister.SlaveID3, i2cAlias);
            deserializer.WriteByte((uint)DS90UB9xDeserializerI2CRegister.SlaveAlias3, i2cAlias);
        }

        internal static void ConfigureSerializer(DeviceContext device)
        {
            DS90UB9x.Set933I2CRate(device, 80e3); //This is an arbitrary value that is proven to work, we need to test speed vs reliability vs bno sampling speed
        }

        internal static void ConfigureCameraSystem(DeviceContext device, UclaMiniscopeV4FramesPerSecond frameRate, bool interleaveLed)
        {
            
            // set up Python480
            ShutDownCamera(device);
            StartUpCamera(device);                        

            // set up potentiometer
            var tpl0102 = new I2CRegisterContext(device, UclaMiniscopeV4.Tpl0102Address);
            tpl0102.WriteByte(0x00, 0x72);
            tpl0102.WriteByte(0x01, 0x00);

            // turn on EWL
            var max14574 = new I2CRegisterContext(device, UclaMiniscopeV4.Max14574Address);
            max14574.WriteByte(0x03, 0x03);

            // configuration properties
            uint shutterWidth = frameRate switch
            {
                UclaMiniscopeV4FramesPerSecond.Fps10Hz => 10000,
                UclaMiniscopeV4FramesPerSecond.Fps15Hz => 6667,
                UclaMiniscopeV4FramesPerSecond.Fps20Hz => 5000,
                UclaMiniscopeV4FramesPerSecond.Fps25Hz => 4000,
                UclaMiniscopeV4FramesPerSecond.Fps30Hz => 3300,
                _ => 3300
            };
            var atMega = new I2CRegisterContext(device, UclaMiniscopeV4.AtMegaAddress)

            atMega.WriteByte(0x04, (uint)(interleaveLed ? 0x00 : 0x03));
            WriteCameraRegister(atMega, 200, shutterWidth);
        }

        internal static void ShutDownCamera(DeviceContext device)
        {
            var atMega = new I2CRegisterContext(device, UclaMiniscopeV4.AtMegaAddress);
            //disable sequencer
            WriteCameraRegister(atMega, 192, 0x0802);
            //soft power down
            WriteCameraRegister(atMega, 112, 0x0000); // Disable LVDS transmitters
            WriteCameraRegister(atMega, 72, 0x0010); // Disable charge pump
            WriteCameraRegister(atMega, 64, 0x0000); // Disable biasing block
            WriteCameraRegister(atMega, 48, 0x0000); // Disable AFE
            WriteCameraRegister(atMega, 42, 0x4110); // Configure image core for shutdown          
            WriteCameraRegister(atMega, 40, 0x0000); // Disable column multiplexer
            WriteCameraRegister(atMega, 32, 0x7006); // Disable analog clock
            WriteCameraRegister(atMega, 10, 0x0999); // Soft reset

            //disable clock mgmt 2
            WriteCameraRegister(atMega, 34, 0x0000); // Disable logic blocks
            WriteCameraRegister(atMega, 32, 0x7004); // Disable logic clock
            WriteCameraRegister(atMega, 9, 0x0009); // Soft reset clock generator
            
            //disable clock mgmt 1
            WriteCameraRegister(atMega, 16, 0x0000); // Disable PLL
            WriteCameraRegister(atMega, 8, 0x0099); // Soft reset PLL

           
        }

        internal static void StartUpCamera(DeviceContext device)
        {
            var atMega = new I2CRegisterContext(device, UclaMiniscopeV4.AtMegaAddress);
            //enable clock mgmt 1
            WriteCameraRegister(atMega, 2, 0x0000); // Set Monochrome sensor
            WriteCameraRegister(atMega, 8, 0x0000); // Release PLL soft reset
            WriteCameraRegister(atMega, 16, 0x0003); // Enable PLL
            WriteCameraRegister(atMega, 17, 0x2113); // Configure PLL
            WriteCameraRegister(atMega, 20, 0x0000); // Configure clock management
            WriteCameraRegister(atMega, 26, 0x2280); // Configure PLL lock detector
            WriteCameraRegister(atMega, 27, 0x3D2D); // Configure PLL lock detector
            WriteCameraRegister(atMega, 32, 0x7014); // Configure clock management
            Thread.Sleep(10); //This might not be needed, remove if it hurts
            
            //enable clock mgmt 2
            WriteCameraRegister(atMega, 9, 0x0000); // Rlease clock generator reset
            WriteCameraRegister(atMega, 32, 0x7006); // Enable logic clock
            WriteCameraRegister(atMega, 34, 0x0001); // Enable logic blocks

            //set format data
            WriteCameraRegister(atMega, 199, 666); // Defines granularity (unit = 1/PLL clock) of exposure and reset_length. PLL clock is 66.666MHz. The 480 docs says it should be 68MHz. Oh, well...
            WriteCameraRegister(atMega, 200, 3300); // Set frame rate to 30 Hz
            WriteCameraRegister(atMega, 201, 3000); // Set Exposure. This is a parameter we might want to play with to tune image quality

            //soft power up
            WriteCameraRegister(atMega, 10, 0x0000); // Release soft reset
            WriteCameraRegister(atMega, 32, 0x7007); // Enable analog clock
            WriteCameraRegister(atMega, 40, 0x0007); // Enable column multiplexer. This is the value used by UCLA, which enables column multiplexer bias. On a reference document I found is set to 0x0003, so not sure what the best setting is
            WriteCameraRegister(atMega, 42, 0x4113); // Configure image core
            WriteCameraRegister(atMega, 48, 0x0001); // Enable AFE
            WriteCameraRegister(atMega, 64, 0x0001); // Enable biasing block
            WriteCameraRegister(atMega, 72, 0x0017); // Enable charge pump
            WriteCameraRegister(atMega, 112, 0x0000); // Disable LVDS transmitters

            //enable sequencer
            WriteCameraRegister(atMega, 192, 0x0803);
        }

        static void WriteCameraRegister(I2CRegisterContext i2c, uint register, uint value)
        {
            // ATMega -> Python480 passthrough protocol
            var regLow = register & 0xFF;
            var regHigh = (register >> 8) & 0xFF;
            var valLow = value & 0xFF;
            var valHigh = (value >> 8) & 0xFF;

            i2c.WriteByte(0x05, regHigh);
            i2c.WriteByte(regLow, valHigh);
            i2c.WriteByte(valLow, 0x00);
        }

        internal static void SetLedBrightness(DeviceContext device, double percent)
        {
            var atMega = new I2CRegisterContext(device, UclaMiniscopeV4.AtMegaAddress);
            atMega.WriteByte(0x01, (uint)((percent == 0) ? 0xFF : 0x08));

            var tpl0102 = new I2CRegisterContext(device, UclaMiniscopeV4.Tpl0102Address);
            tpl0102.WriteByte(0x01, (uint)(255 * ((100 - percent) / 100.0)));
        }

        internal static void SetSensorGain(DeviceContext device, UclaMiniscopeV4SensorGain gain)
        {
            var atMega = new I2CRegisterContext(device, UclaMiniscopeV4.AtMegaAddress);
            WriteCameraRegister(atMega, 204, (uint)gain);
        }

        internal static void SetLiquidLensVoltage(DeviceContext device, double voltage)
        {
            var max14574 = new I2CRegisterContext(device, UclaMiniscopeV4.Max14574Address);
            max14574.WriteByte(0x08, (uint)((voltage - 24.4) / 0.0445) >> 2);
            max14574.WriteByte(0x09, 0x02);
        }
    }

    static class UclaMiniscopeV4
    {
        public const int AtMegaAddress = 0x10;
        public const int Tpl0102Address = 0x50;
        public const int Max14574Address = 0x77;

        public const int SensorRows = 608;
        public const int SensorColumns = 608;

        internal class NameConverter : DeviceNameConverter
        {
            public NameConverter()
                : base(typeof(UclaMiniscopeV4))
            {
            }
        }
    }

    /// <summary>
    /// Specifies analog gain of the Python-480 image sensor on a UCLA Miniscope V4.
    /// </summary>
    public enum UclaMiniscopeV4SensorGain
    {
        /// <summary>
        /// Specifies low gain.
        /// </summary>
        Low = 0x00E1,

        /// <summary>
        /// Specifies medium gain.
        /// </summary>
        Medium = 0x00E4,

        /// <summary>
        /// Specifies high gain.
        /// </summary>
        High = 0x0024,
    }

    /// <summary>
    /// Specifies the video frame rate of the Python-480 image sensor on a UCLA Miniscope V4.
    /// </summary>
    public enum UclaMiniscopeV4FramesPerSecond
    {
        /// <summary>
        /// Specifies 10 frames per second.
        /// </summary>
        Fps10Hz,

        /// <summary>
        /// Specifies 15 frames per second.
        /// </summary>
        Fps15Hz,

        /// <summary>
        /// Specifies 20 frames per second.
        /// </summary>
        Fps20Hz,

        /// <summary>
        /// Specifies 25 frames per second.
        /// </summary>
        Fps25Hz,

        /// <summary>
        /// Specifies 30 frames per second.
        /// </summary>
        Fps30Hz,
    }
}
