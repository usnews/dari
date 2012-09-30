if (typeof jQuery !== 'undefined') (function($) {

$.plugin('repeatable', {
    'init': function(options) {
        return this.liveInit(function() {
            var $container = $(this);
            var $items = $container.find('.repeatable-item');
            var $template = $container.find('.repeatable-template');

            var appendRemove = function() {
                $(this).append($('<a/>', {
                    'class': 'repeatable-remove',
                    'href': '#',
                    'text': 'Remove',
                    'click': function() {
                        $(this).parent().remove();
                        return false;
                    }
                }));
            };

            $items.each(appendRemove);

            var $add = $('<a/>', {
                'class': 'repeatable-add',
                'href': '#',
                'text': 'Add',
                'click': function() {
                    var $clone = $template.clone();
                    $add.before($clone);
                    appendRemove.apply($clone);
                    return false;
                }
            });

            $template.after($add);
            $template.remove();
        });
    }
});

})(jQuery);
