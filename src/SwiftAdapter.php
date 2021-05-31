<?php

namespace Nimbusoft\Flysystem\OpenStack;

use GuzzleHttp\Psr7\Stream;
use GuzzleHttp\Psr7\StreamWrapper;
use League\Flysystem\Config;
use League\Flysystem\DirectoryAttributes;
use League\Flysystem\FileAttributes;
use League\Flysystem\FilesystemAdapter;
use League\Flysystem\FilesystemException;
use League\Flysystem\FilesystemOperationFailed;
use League\Flysystem\InvalidVisibilityProvided;
use League\Flysystem\PathPrefixer;
use League\Flysystem\StorageAttributes;
use League\Flysystem\UnableToCheckFileExistence;
use League\Flysystem\UnableToCopyFile;
use League\Flysystem\UnableToCreateDirectory;
use League\Flysystem\UnableToDeleteDirectory;
use League\Flysystem\UnableToDeleteFile;
use League\Flysystem\UnableToMoveFile;
use League\Flysystem\UnableToReadFile;
use League\Flysystem\UnableToRetrieveMetadata;
use League\Flysystem\UnableToSetVisibility;
use League\Flysystem\UnableToWriteFile;
use League\Flysystem\UnixVisibility\PortableVisibilityConverter;
use League\Flysystem\UnixVisibility\VisibilityConverter;
use League\Flysystem\Visibility;
use League\MimeTypeDetection\FinfoMimeTypeDetector;
use League\MimeTypeDetection\MimeTypeDetector;
use OpenStack\Common\Error\BadResponseError;
use OpenStack\ObjectStore\v1\Models\Container;
use OpenStack\ObjectStore\v1\Models\StorageObject;

class SwiftAdapter implements FilesystemAdapter
{

    /**
     * @var Container
     */
    protected Container $container;

    /**
     * @var PathPrefixer
     */
    private PathPrefixer $prefixer;

    /**
     * @var PortableVisibilityConverter|VisibilityConverter
     */
    private $visibility;

    /**
     * @var FinfoMimeTypeDetector|MimeTypeDetector
     */
    private $mimeTypeDetector;

    /**
     * Constructor
     *
     * @param Container $container
     * @param string    $prefix
     */
    public function __construct(Container $container,
                                VisibilityConverter $visibility = null,
                                MimeTypeDetector $mimeTypeDetector = null,
                                $prefix = null)
    {
        $this->prefixer = new PathPrefixer($prefix);
        $this->container = $container;
        $this->visibility = $visibility ?: new PortableVisibilityConverter();
        $this->mimeTypeDetector = $mimeTypeDetector ?: new FinfoMimeTypeDetector();
    }

    /**
     * {@inheritdoc}
     */
    public function write($path, $contents, Config $config): void
    {
        $path = $this->prefixer->prefixPath($path);
        // FIXME a size was passed in, is it efficient to get the size of a large $contents?
        $size = 0;

        $data = $this->getWriteData($path, $config);
        $type = 'content';

        if (is_a($contents, 'GuzzleHttp\Psr7\Stream')) {
            $type = 'stream';
        }

        $data[$type] = $contents;

        // Create large object if the stream is larger than 300 MiB (default).
        if ($type === 'stream' && $size > $config->get('swiftLargeObjectThreshold', 314572800)) {
            // Set the segment size to 100 MiB by default as suggested in OVH docs.
            $data['segmentSize'] = $config->get('swiftSegmentSize', 104857600);
            // Set segment container to the same container by default.
            $data['segmentContainer'] = $config->get('swiftSegmentContainer', $this->container->name);

            $response = $this->container->createLargeObject($data);
        } else {
            $response = $this->container->createObject($data);
        }

        //return $this->normalizeObject($response);
    }

    /**
     * {@inheritdoc}
     */
    public function writeStream($path, $resource, Config $config): void
    {
        // No more getStreamSize
        $this->write($path, new Stream($resource), $config);
    }

    /**
     * {@inheritdoc}
     */
    public function move(string $path, string $newpath, Config $config): void
    {
        $object = $this->getObject($path);
        $newLocation = $this->prefixer->prefixPath($newpath);
        $destination = '/'.$this->container->name.'/'.ltrim($newLocation, '/');

        try {
            $object->copy(compact('destination'));
        } catch (BadResponseError $e) {
            throw UnableToMoveFile::fromLocationTo($path, $newpath);
        }

        $object->delete();
    }

    /**
     * {@inheritdoc}
     */
    public function delete($path): void
    {
        $object = $this->getObjectInstance($path);

        try {
            $object->delete();
        } catch (BadResponseError $e) {
            throw UnableToDeleteFile::atLocation($path);
        }
    }

    /**
     * {@inheritdoc}
     */
    public function deleteDir($dirname)
    {
        // Make sure a slash is added to the end.
        $dirname = rtrim(trim($dirname), '/') . '/';

        // To be safe, don't delete everything.
        if($dirname === '/') {
            return false;
        }

        $objects = $this->container->listObjects([
            'prefix' => $this->prefixer->prefixPath($dirname)
        ]);

        try {
            foreach ($objects as $object) {
                $object->containerName = $this->container->name;
                $object->delete();
            }
        } catch (BadResponseError $e) {
            return false;
        }

        return true;
    }

    /**
     * {@inheritdoc}
     */
    public function has($path)
    {
        try {
            $object = $this->getObject($path);
        } catch (BadResponseError $e) {
            $code = $e->getResponse()->getStatusCode();

            if ($code == 404) return false;

            throw $e;
        }

        return $this->normalizeObject($object);
    }

    /**
     * {@inheritdoc}
     */
    public function read($path): string
    {
        $object = $this->getObject($path);
        $data = $this->normalizeObject($object);

        $stream = $object->download();
        $stream->rewind();
        return $stream->getContents();
    }

    /**
    * {@inheritdoc}
    */
    public function readStream($path)
    {
       $object = $this->getObject($path);
       $data = $this->normalizeObject($object);

       $stream = $object->download();
       $stream->rewind();
       $data['stream'] = StreamWrapper::getResource($stream);

       return $data;
    }

    /**
     * {@inheritdoc}
     */
    public function listContents($path = '', $deep = false): iterable
    {
        // AWS adapter uses generators to yield results
        $location = $this->prefixer->prefixPath($path);

        $objectList = $this->container->listObjects([
            'prefix' => $location
        ]);

        foreach($objectList as $object) {
            yield $this->normalizeObject($object);
        }
    }

    /**
     * {@inheritdoc}
     */
    public function getMetadata($path)
    {
        $object = $this->getObject($path);

        return $this->normalizeObject($object);
    }

    /**
     * {@inheritdoc}
     */
    public function fileSize($path): FileAttributes
    {
        return $this->getMetadata($path);
    }

    /**
     * {@inheritdoc}
     */
    public function getMimetype($path)
    {
        return $this->getMetadata($path);
    }

    /**
     * {@inheritdoc}
     */
    public function getTimestamp($path)
    {
        return $this->getMetadata($path);
    }

    /**
     * Get the data properties to write or update an object.
     *
     * @param string $path
     * @param Config $config
     *
     * @return array
     */
    protected function getWriteData($path, $config)
    {
        return ['name' => $path];
    }

    /**
     * Get an object instance.
     *
     * @param string $path
     *
     * @return StorageObject
     */
    protected function getObjectInstance($path)
    {
        $location = $this->prefixer->prefixPath($path);

        $object = $this->container->getObject($location);

        return $object;
    }

    /**
     * Get an object instance and retrieve its metadata from storage.
     *
     * @param string $path
     *
     * @return StorageObject
     */
    protected function getObject($path)
    {
        $object = $this->getObjectInstance($path);
        $object->retrieve();

        return $object;
    }

    /**
     * Normalize Openstack "StorageObject" object into an array
     *
     * @param StorageObject $object
     * @return array
     */
    protected function normalizeObject(StorageObject $object): FileAttributes
    {
        $name = $this->prefixer->stripPrefix($object->name);
        $mimetype = explode('; ', $object->contentType);

        if ($object->lastModified instanceof \DateTimeInterface) {
            $timestamp = $object->lastModified->getTimestamp();
        } else {
            $timestamp = strtotime($object->lastModified);
        }

        $attribs = [
            'type'      => 'file',
            'dirname'   => $this->prefixer->prefixPath($object->name),
            'path'      => $name,
            'timestamp' => $timestamp,
            'mimetype'  => reset($mimetype),
            'size'      => $object->contentLength,
        ];
        return new FileAttributes($name, $object->contentLength, null, $timestamp, $object->contentType );
    }

    public function fileExists(string $path): bool
    {
        try {
            return $this->container->objectExists($this->prefixer->prefixPath($path));
        } catch (\Throwable $exception) {
            throw UnableToCheckFileExistence::forLocation($path, $exception);
        }
    }

    public function deleteDirectory(string $path): void
    {
        // TODO: Implement deleteDirectory() method.
        throw UnableToDeleteDirectory::atLocation($path);
    }

    public function createDirectory(string $path, Config $config): void
    {
        // Directories are implicitly created
    }

    public function setVisibility(string $path, string $visibility): void
    {
        // TODO: Implement setVisibility() method.
        UnableToSetVisibility::atLocation($path);

    }

    public function visibility(string $path): FileAttributes
    {
        // TODO: Implement visibility() method.
        UnableToRetrieveMetadata::visibility($path);
    }

    public function mimeType(string $path): FileAttributes
    {
        // TODO: Implement mimeType() method.
        try {
            $object = $this->getObject($path);
            return $this->normalizeObject($object);
        } catch( \Throwable $exception) {
            throw UnableToRetrieveMetadata::mimeType($path);
        }
    }

    public function lastModified(string $path): FileAttributes
    {
        // TODO: Implement lastModified() method.
        try {
            $object = $this->getObject($path);
            return $this->normalizeObject($object);
        } catch( \Throwable $exception) {
            throw UnableToRetrieveMetadata::lastModified($path);
        }
    }

    public function copy(string $source, string $destination, Config $config): void
    {
        // TODO: Implement copy() method.
        throw UnableToCopyFile::fromLocationTo($source, $destination);
    }
}
