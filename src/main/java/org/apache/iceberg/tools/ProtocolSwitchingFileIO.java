package org.apache.iceberg.tools;

import org.apache.iceberg.ManifestFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SeekableInputStream;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ProtocolSwitchingFileIO implements FileIO {

    private static final Logger LOG = LoggerFactory.getLogger(ProtocolSwitchingFileIO.class);
    private static final String VERSION = "1.0.0";
    private static final String DELEGATE_FILE_IO_CLASS = "io-impl-delegate";

    private FileIO delegateFileIO;
    private Map<String, String> protocolMappings;

    public ProtocolSwitchingFileIO() {
        LOG.info("Initializing ProtocolSwitchingFileIO version: {}", VERSION);
        this.protocolMappings = new HashMap<>();
    }

    public static String getVersion() {
        System.out.println("Initializing ProtocolSwitchingFileIO version: {}" + VERSION);
        return VERSION;
    }

    @Override
    public void initialize(Map<String, String> properties) {
        LOG.info("Initializing ProtocolSwitchingFileIO with properties.");

        // Get the delegate FileIO class name from properties
        String delegateClassName = properties.get(DELEGATE_FILE_IO_CLASS);
        if (delegateClassName == null || delegateClassName.isEmpty()) {
            throw new IllegalArgumentException("Delegate FileIO class name must be specified in property '"
                    + DELEGATE_FILE_IO_CLASS + "'");
        }

        try {
            Class<?> clazz = Class.forName(delegateClassName);
            if (!FileIO.class.isAssignableFrom(clazz)) {
                throw new IllegalArgumentException("Class " + delegateClassName + " does not implement FileIO.");
            }
            delegateFileIO = (FileIO) clazz.getDeclaredConstructor().newInstance();

            Map<String, String> mutableProps = new HashMap<>(properties);

            for (Map.Entry<String, String> entry : properties.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();

                // Check conditions
                if (key.startsWith("adls.sas-token")
                        && !key.endsWith(".blob.core.windows.net")
                        && !key.endsWith(".dfs.core.windows.net")) {

                    String newKey = key + ".blob.core.windows.net";

                    // Avoid overwriting existing keys
                    if (!mutableProps.containsKey(newKey)) {
                        mutableProps.put(newKey, value);
                        LOG.info("Added derived property: {} -> {}", newKey, value);
                    }
                }
            }

            delegateFileIO.initialize(mutableProps);
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize delegate FileIO: " + delegateClassName, e);
        }

        // Load protocol mappings from properties
        protocolMappings = parseProtocolMappings(properties);
    }

    @Override
    public InputFile newInputFile(String location) {
        LOG.debug("Creating new InputFile for location: {}", location);
        String adjustedLocation = adjustLocation(location);
        return new SwitchedInputFile(delegateFileIO.newInputFile(adjustedLocation), location);
    }

    @Override
    public InputFile newInputFile(String location, long length) {
        LOG.debug("Creating new InputFile with length for location: {}", location);
        String adjustedLocation = adjustLocation(location);
        return new SwitchedInputFile(delegateFileIO.newInputFile(adjustedLocation, length), location);
    }

    @Override
    public OutputFile newOutputFile(String location) {
        LOG.debug("Creating new OutputFile for location: {}", location);
        String adjustedLocation = adjustLocation(location);
        return new SwitchedOutputFile(delegateFileIO.newOutputFile(adjustedLocation), location);
    }

    @Override
    public void deleteFile(String location) {
        LOG.debug("Deleting file at location: {}", location);
        String adjustedLocation = adjustLocation(location);
        delegateFileIO.deleteFile(adjustedLocation);
    }

    @Override
    public InputFile newInputFile(ManifestFile manifest) {
        LOG.debug("Creating new InputFile for manifest path: {}", manifest.path());
        String adjustedLocation = adjustLocation(manifest.path());
        return new SwitchedInputFile(delegateFileIO.newInputFile(adjustedLocation, manifest.length()), manifest.path());
    }

    private String adjustLocation(String location) {
        for (Map.Entry<String, String> entry : protocolMappings.entrySet()) {
            String regex = entry.getKey();
            String replacement = entry.getValue();
            Matcher matcher = Pattern.compile(regex).matcher(location);
            if (matcher.find()) {
                String newLocation = matcher.replaceFirst(replacement);
                LOG.debug("Adjusted location from '{}' to '{}'", location, newLocation);
                return newLocation;
            }
        }
        return location; // No conversion needed
    }

    private Map<String, String> parseProtocolMappings(Map<String, String> properties) {
        Map<String, String> mappings = new HashMap<>();
        properties.forEach((key, value) -> {
            if (key.startsWith("protocol.mapping.")) {
                String regex = key.substring("protocol.mapping.".length());
                mappings.put(regex, value);
            }
        });
        LOG.debug("Protocol mappings: {}", mappings);
        return mappings;
    }

    private static class SwitchedInputFile implements InputFile {
        private final InputFile delegate;
        private final String originalLocation;

        public SwitchedInputFile(InputFile delegate, String originalLocation) {
            this.delegate = delegate;
            this.originalLocation = originalLocation;
        }

        @Override
        public long getLength() {
            return delegate.getLength();
        }

        @Override
        public String location() {
            return originalLocation;
        }

        @Override
        public SeekableInputStream newStream() {
            return delegate.newStream();
        }

        @Override
        public boolean exists() {
            return delegate.exists();
        }
    }

    private static class SwitchedOutputFile implements OutputFile {
        private final OutputFile delegate;
        private final String originalLocation;

        public SwitchedOutputFile(OutputFile delegate, String originalLocation) {
            this.delegate = delegate;
            this.originalLocation = originalLocation;
        }

        @Override
        public PositionOutputStream create() {
            return delegate.create();
        }

        @Override
        public PositionOutputStream createOrOverwrite() {
            return delegate.createOrOverwrite();
        }

        @Override
        public String location() {
            return originalLocation;
        }

        @Override
        public InputFile toInputFile() {
            return new SwitchedInputFile(delegate.toInputFile(), originalLocation);
        }
    }
}
