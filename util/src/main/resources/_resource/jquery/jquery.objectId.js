if (typeof jQuery !== 'undefined') (function($) {

$.plugin('objectId', {
    'init': function(options) {
        return this.liveInit(function() {
            var $input = $(this);

            var $select = $('<a/>', {
                'class': 'objectId-select popup',
                'href': location.pathname + '?action=select&from=' + $input.attr('data-type-ids'),
                'target': 'objectId-select',
                'text': 'Select'
            });

            $input.after($select);
        });
    }
});

})(jQuery);
