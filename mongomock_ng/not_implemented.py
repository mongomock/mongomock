"""Module to handle features that are not implemented yet."""

import warnings


_IGNORED_FEATURES = {
    'array_filters': False,
    'collation': False,
    'let': False,
    'comment': True,
    'hint': True,
}


def _ensure_ignorable_feature(feature):
    if feature not in _IGNORED_FEATURES:
        raise KeyError(
            f'{feature} is not an error that can be ignored: maybe it has been implemented in '
            f'Mongomock-ng. Here is the list of features that can be ignored: '
            f'{_IGNORED_FEATURES.keys()}'
        )


def ignore_feature(feature):
    """Ignore a feature instead of raising a NotImplementedError."""
    _ensure_ignorable_feature(feature)
    _IGNORED_FEATURES[feature] = True


def warn_on_feature(feature):
    """Rasie a NotImplementedError the next times a feature is used."""
    _ensure_ignorable_feature(feature)
    _IGNORED_FEATURES[feature] = False


def raise_for_feature(feature, reason):
    _ensure_ignorable_feature(feature)
    if _IGNORED_FEATURES[feature]:
        warnings.warn(
            f"'{feature}' is accepted for pymongo compatibility but is a no-op "
            f'in mongomock-ng: {reason}',
            UserWarning,
            stacklevel=3,
        )
        return False
    raise NotImplementedError(reason)
