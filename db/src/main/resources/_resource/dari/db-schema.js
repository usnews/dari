$(function() {

    // Try to assign a unique color to all different types.
    var goldenRatio = 0.618033988749895;
    var globalHue = Math.random();
    $('.type').each(function() {
        var $type = $(this);
        var typeColor = $type.attr('data-typeColor');
        if (!typeColor) {
            globalHue += goldenRatio;
            globalHue %= 1.0;
            typeColor = 'hsl(' + (globalHue * 360) + ', 50%, 50%)';
            $type.attr('data-typeColor', typeColor);
            $type.css('border-color', typeColor);
            $type.find('h2').css('color', typeColor);
        }
    });

    var $document = $(document);
    var $pathCanvas = $('<canvas/>');
    $pathCanvas.attr({
        'width': $document.width(),
        'height': $document.height()
    });
    $pathCanvas.css({
        'left': '0px',
        'position': 'absolute',
        'top': '0px',
        'z-index': -1
    });

    $(document.body).append($pathCanvas);
    var pathCanvas = $pathCanvas[0].getContext('2d');

    // Draw a nice curved line between type definitions and their references.
    $('.type .reference').each(function() {
        var $typeRef = $(this);

        var $target = $('#type-' + $typeRef.attr('data-typeId'));
        var targetTypeColor = $target.attr('data-typeColor');
        $typeRef.css('background-color', targetTypeColor);

        if ($target.length == 0 || !this || $.contains($target[0], this)) {
            return;
        }

        var pathSourceX, pathSourceY, pathSourceDirection;
        var pathTargetX, pathTargetY, pathTargetDirection;
        var sourceOffset = $typeRef.offset();
        var targetOffset = $target.offset();
        var isBackReference = false;

        if (sourceOffset.left > targetOffset.left) {
            var targetWidth = $target.outerWidth();
            pathTargetX = targetOffset.left + targetWidth;
            pathTargetY = targetOffset.top + $target.outerHeight() / 2;
            isBackReference = true;

            if (targetOffset.left + targetWidth > sourceOffset.left) {
                pathSourceX = sourceOffset.left + $typeRef.width();
                pathSourceY = sourceOffset.top + $typeRef.height() / 2;
                pathSourceDirection = 1;
                pathTargetDirection = 1;

            } else {
                pathSourceX = sourceOffset.left;
                pathSourceY = sourceOffset.top + $typeRef.height() / 2;
                pathSourceDirection = -1;
                pathTargetDirection = 1;
            }

        } else {
            pathSourceX = sourceOffset.left + $typeRef.width();
            pathSourceY = sourceOffset.top + $typeRef.height() / 2;
            pathTargetX = targetOffset.left;
            pathTargetY = targetOffset.top + $target.height() / 2;
            pathSourceDirection = 1;
            pathTargetDirection = -1;
        }

        var pathSourceControlX = pathSourceX + pathSourceDirection * 100;
        var pathSourceControlY = pathSourceY;
        var pathTargetControlX = pathTargetX + pathTargetDirection * 100;
        var pathTargetControlY = pathTargetY;

        pathCanvas.strokeStyle = targetTypeColor;
        pathCanvas.fillStyle = targetTypeColor;

        // Reference curve.
        pathCanvas.lineWidth = isBackReference ? 0.4 : 1.0;
        pathCanvas.beginPath();
        pathCanvas.moveTo(pathSourceX, pathSourceY);
        pathCanvas.bezierCurveTo(pathSourceControlX, pathSourceControlY, pathTargetControlX, pathTargetControlY, pathTargetX, pathTargetY);
        pathCanvas.stroke();

        // Arrow head.
        var arrowSize = pathTargetX > pathTargetControlX ? 5 : -5;
        if (isBackReference) {
            arrowSize *= 0.8;
        }
        pathCanvas.beginPath();
        pathCanvas.moveTo(pathTargetX, pathTargetY);
        pathCanvas.lineTo(pathTargetX - 2 * arrowSize, pathTargetY - arrowSize);
        pathCanvas.lineTo(pathTargetX - 2 * arrowSize, pathTargetY + arrowSize);
        pathCanvas.closePath();
        pathCanvas.fill();
    });
});
