document.addEventListener('DOMContentLoaded', () => {
    const hamburgerButton = document.querySelector('.hamburger-menu');
    const mobileNavMenu = document.querySelector('.mobile-nav-menu');
    const siteHeader = document.querySelector('.site-header'); // Get the header

    if (hamburgerButton && mobileNavMenu && siteHeader) {
        hamburgerButton.addEventListener('click', () => {
            const isActive = mobileNavMenu.classList.contains('is-active');

            if (isActive) {
                // Closing the menu
                mobileNavMenu.classList.remove('is-active');
                hamburgerButton.classList.remove('is-active');
                hamburgerButton.setAttribute('aria-expanded', 'false');
                document.body.style.overflow = ''; // Restore body scrolling
            } else {
                // Opening the menu
                // Adjust top position of mobile menu to be below the actual header
                const headerHeight = siteHeader.offsetHeight;
                mobileNavMenu.style.top = `${headerHeight}px`;
                // Adjust height to not overlap with fixed header
                mobileNavMenu.style.height = `calc(100vh - ${headerHeight}px)`;

                mobileNavMenu.classList.add('is-active');
                hamburgerButton.classList.add('is-active');
                hamburgerButton.setAttribute('aria-expanded', 'true');
                document.body.style.overflow = 'hidden'; // Prevent body scrolling when menu is open
            }
        });
    }
});
