from rest_framework import authentication


class DRFAuthentication(authentication.BaseAuthentication):
    def authenticate(self, request):
        return 'none', 'none'
