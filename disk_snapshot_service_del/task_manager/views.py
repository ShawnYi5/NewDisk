from rest_framework.response import Response
from rest_framework.views import APIView


class HostSnapshotView(APIView):

    def post(self, request):
        print('post~~~~~~~~~~~~')
        return Response()
