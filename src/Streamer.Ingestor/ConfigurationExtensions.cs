using System;
using System.Collections.Generic;
using System.Text;

namespace Streamer.Ingestor.Extensions
{
    public static class ConfigurationExtensions
    {
        public static int TryGetAsInt(this System.Fabric.Description.ConfigurationSection section, string name, int @default)
        {
            if (section.Parameters.Contains(name))
            {
                var value = section.Parameters[name];
                if (Int32.TryParse(value.Value, out int result))
                    return result;
            }

            return @default;
        }

        public static string Get(this System.Fabric.Description.ConfigurationSection section, string name)
        {
            if (section.Parameters.Contains(name))
            {
                var val = section.Parameters[name];

                if (!String.IsNullOrEmpty(val.Value))
                    return val.Value;
            }

            throw new ArgumentNullException($"The configuration value of {name} was null or empty but is required.");
        }
    }
}
