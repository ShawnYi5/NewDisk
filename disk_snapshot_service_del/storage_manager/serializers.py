from rest_framework import serializers

class DiskSnapshotStorageForImageOpenSerializer(serializers.Serializer):
    image_path = serializers.CharField(max_length=128)
    snapshot_name = serializers.CharField(max_length=32)

